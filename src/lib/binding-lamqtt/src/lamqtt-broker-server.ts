/********************************************************************************
 * Copyright (c) 2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v. 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the W3C Software Notice and
 * Document License (2015-05-13) which is available at
 * https://www.w3.org/Consortium/Legal/2015/copyright-software-and-document.
 *
 * SPDX-License-Identifier: EPL-2.0 OR W3C-20150513
 ********************************************************************************/

/**
 * MQTT Broker Server
 */

import { IPublishPacket } from "mqtt";
import * as mqtt from "mqtt";
import * as url from "url";
import * as TD from "@node-wot/td-tools";
import { LAMqttBrokerServerConfig, LAMqttLocationProvider } from "./lamqtt";
import {
  ProtocolServer,
  Servient,
  ExposedThing,
  ContentSerdes,
  ProtocolHelpers,
  Content,
  createLoggers,
} from "@node-wot/core";
import { InteractionOptions } from "wot-typescript-definitions";
import { ActionElement, PropertyElement } from "wot-thing-description-types";
import { MQTTMessage, MQTTReceiver, SpatialMQTTClient } from "la-mqtt";

const { info, debug, error, warn } = createLoggers(
  "binding-lamqtt",
  "lamqtt-broker-server"
);

export default class LAMqttBrokerServer implements ProtocolServer {
  readonly scheme: string = "mqtt";

  private port = -1;
  private address: string = undefined;

  private brokerURI: string = undefined;
  private locationProvider: LAMqttLocationProvider;
  private locationUpdateHandler: NodeJS.Timer;

  private readonly things: Map<string, ExposedThing> = new Map();

  private readonly config: LAMqttBrokerServerConfig;

  private broker: SpatialMQTTClient;

  constructor(config: LAMqttBrokerServerConfig) {
    this.config = config ?? {
      uri: "mqtt://localhost:1883",
      clientId: "localClient",
      locationProvider: () => {
        return { latitude: 0, longitude: 0 };
      },
    };

    if (config.uri !== undefined) {
      // if there is a MQTT protocol indicator missing, add this
      if (config.uri.indexOf("://") === -1) {
        config.uri = this.scheme + "://" + config.uri;
      }
      this.brokerURI = config.uri;
    }
  }

  public async expose(thing: ExposedThing): Promise<void> {
    if (this.broker === undefined) {
      return;
    }

    let name = thing.title;

    if (this.things.has(name)) {
      const suffix = name.match(/.+_([0-9]+)$/);
      if (suffix !== null) {
        name = name.slice(0, -suffix[1].length) + (1 + parseInt(suffix[1]));
      } else {
        name = name + "_2";
      }
    }

    debug(
      `LAMqttBrokerServer at ${this.brokerURI} exposes '${thing.title}' as unique '${name}/*'`
    );

    this.things.set(name, thing);

    for (const propertyName in thing.properties) {
      await this.exposeProperty(name, propertyName, thing);
    }

    for (const actionName in thing.actions) {
      this.exposeAction2(name, actionName, thing);
    }

    for (const eventName in thing.events) {
      this.exposeEvent2(name, eventName, thing);
    }

    //TODO
    //this.broker.publish(name, JSON.stringify(thing.getThingDescription()), {
    //  retain: true,
    //});
  }

  private async exposeProperty(
    name: string,
    propertyName: string,
    thing: ExposedThing
  ) {
    const property = thing.properties[propertyName];
    const rootTopic =
      encodeURIComponent(name) +
      "/properties/" +
      encodeURIComponent(propertyName);
    if (!property.writeOnly) {
      // Property can be read
      const askTopic = rootTopic + "/readAsk";
      const answerTopic = rootTopic + "/readAnswer";
      let askCallback: MQTTReceiver = { messageRecv() {} };
      askCallback.messageRecv = async (message) => {
        debug(
          `LAMqttBrokerServer at ${this.brokerURI} received message for '${message.topic}'`
        );
        const content = await thing.handleReadProperty(propertyName, {
          formIndex: property.forms.length - 1,
        });
        const data = await content.toBuffer();
        this.broker.publish(answerTopic, data.toString());
      };
      await this.broker.subscribeGeofence(askTopic, askCallback);
      /// Create form
      const askHref = this.brokerURI + "/" + askTopic;
      const form = new TD.Form(askHref, ContentSerdes.DEFAULT);
      form.op = ["readproperty"];
      form["lamqv:answerTopic"] = answerTopic;
      property.forms.push(form);
      debug(
        `LAMqttBrokerServer at ${this.brokerURI} assigns '${askHref}' to property '${propertyName}'`
      );
    }
    if (!property.readOnly) {
      // Property can be written
      const askTopic = rootTopic + "/writeAsk";
      const answerTopic = rootTopic + "/writeAnswer";
      let askCallback: MQTTReceiver = { messageRecv() {} };
      askCallback.messageRecv = async (message) => {
        debug(
          `LAMqttBrokerServer at ${this.brokerURI} received message for '${message.topic}'`
        );
        await thing.handleWriteProperty(
          propertyName,
          JSON.parse(message.getContent()),
          {
            formIndex: property.forms.length - 1,
          }
        );
        this.broker.publish(answerTopic, "");
      };
      await this.broker.subscribeGeofence(askTopic, askCallback);
      /// Create form
      const askHref = this.brokerURI + "/" + askTopic;
      const form = new TD.Form(askHref, ContentSerdes.DEFAULT);
      form.op = ["writeproperty"];
      form["lamqv:answerTopic"] = answerTopic;
      thing.properties[propertyName].forms.push(form);
      debug(
        `LAMqttBrokerServer at ${this.brokerURI} assigns '${askHref}' to property '${propertyName}'`
      );
    }
  }

  private exposeAction2(
    name: string,
    actionName: string,
    thing: ExposedThing
  ) {}

  private exposeAction(name: string, actionName: string, thing: ExposedThing) {
    const topic =
      encodeURIComponent(name) + "/actions/" + encodeURIComponent(actionName);
    this.broker.subscribe(topic);

    const href = this.brokerURI + "/" + topic;
    const form = new TD.Form(href, ContentSerdes.DEFAULT);
    form.op = ["invokeaction"];
    thing.actions[actionName].forms.push(form);
    debug(
      `LAMqttBrokerServer at ${this.brokerURI} assigns '${href}' to Action '${actionName}'`
    );
  }

  private exposeEvent2(name: string, eventName: string, thing: ExposedThing) {}

  private exposeEvent(name: string, eventName: string, thing: ExposedThing) {
    const topic =
      encodeURIComponent(name) + "/events/" + encodeURIComponent(eventName);
    const event = thing.events[eventName];

    const href = this.brokerURI + "/" + topic;
    const form = new TD.Form(href, ContentSerdes.DEFAULT);
    form.op = ["subscribeevent", "unsubscribeevent"];
    event.forms.push(form);
    debug(
      `LAMqttBrokerServer at ${this.brokerURI} assigns '${href}' to Event '${eventName}'`
    );

    const eventListener = async (content: Content) => {
      if (!content) {
        warn(
          `LAMqttBrokerServer on port ${this.getPort()} cannot process data for Event ${eventName}`
        );
        thing.handleUnsubscribeEvent(eventName, eventListener, {
          formIndex: event.forms.length - 1,
        });
        return;
      }
      debug(
        `LAMqttBrokerServer at ${this.brokerURI} publishing to Event topic '${eventName}' `
      );
      const buffer = await ProtocolHelpers.readStreamFully(content.body);
      //this.broker.publish(topic, buffer);
    };
    thing.handleSubscribeEvent(eventName, eventListener, {
      formIndex: event.forms.length - 1,
    });
  }

  private handleAction(
    action: ActionElement,
    packet: IPublishPacket,
    payload: Buffer,
    segments: string[],
    thing: ExposedThing
  ) {
    /*
     * Currently, this branch will never be taken. The main reason for that is in the mqtt library we use:
     * https://github.com/mqttjs/MQTT.js/pull/1103
     * For further discussion see https://github.com/eclipse/thingweb.node-wot/pull/253
     */
    let value;
    if ("properties" in packet && "contentType" in packet.properties) {
      try {
        value = ContentSerdes.get().contentToValue(
          { type: packet.properties.contentType, body: payload },
          action.input
        );
      } catch (err) {
        warn(
          `LAMqttBrokerServer at ${this.brokerURI} cannot process received message for '${segments[3]}': ${err.message}`
        );
      }
    } else {
      try {
        value = JSON.parse(payload.toString());
      } catch (err) {
        warn(
          `LAMqttBrokerServer at ${this.brokerURI}, packet has no Content Type and does not parse as JSON, relaying raw (string) payload.`
        );
        value = payload.toString();
      }
    }
    const options: InteractionOptions & { formIndex: number } = {
      formIndex: ProtocolHelpers.findRequestMatchingFormIndex(
        action.forms,
        this.scheme,
        this.brokerURI,
        ContentSerdes.DEFAULT
      ),
    };

    thing
      .handleInvokeAction(segments[3], value, options)
      .then((output: unknown) => {
        if (output) {
          warn(
            `LAMqttBrokerServer at ${this.brokerURI} cannot return output '${segments[3]}'`
          );
        }
      })
      .catch((err: Error) => {
        error(
          `LAMqttBrokerServer at ${this.brokerURI} got error on invoking '${segments[3]}': ${err.message}`
        );
      });
  }

  public async destroy(thingId: string): Promise<boolean> {
    debug(
      `LAMqttBrokerServer on port ${this.getPort()} destroying thingId '${thingId}'`
    );
    let removedThing: ExposedThing;
    for (const name of Array.from(this.things.keys())) {
      const expThing = this.things.get(name);
      if (expThing != null && expThing.id != null && expThing.id === thingId) {
        this.things.delete(name);
        removedThing = expThing;
      }
    }
    if (removedThing) {
      info(`LAMqttBrokerServer succesfully destroyed '${removedThing.title}'`);
    } else {
      info(
        `LAMqttBrokerServer failed to destroy thing with thingId '${thingId}'`
      );
    }
    return removedThing !== undefined;
  }

  public async start(servient: Servient): Promise<void> {
    return new Promise<void>(async (resolve, reject) => {
      if (this.brokerURI === undefined) {
        warn(`No broker defined for MQTT server binding - skipping`);
        resolve();
      } else {
        debug(
          `LAMqttBrokerServer trying to connect to secured broker at ${this.brokerURI} with client ID ${this.config.clientId}`
        );

        // TODO: verify password and username
        // TODO: generate random clientID if not provided

        const parsed = new url.URL(this.brokerURI);
        const address = parsed.href;
        const parsedPort = parseInt(parsed.port);
        const port = parsedPort > 0 ? parsedPort : 1883;

        this.broker = new SpatialMQTTClient(
          "username",
          "password",
          address,
          port,
          this.config.clientId
        );

        if (await this.broker.connect()) {
          info(`LAMqttBrokerServer connected to broker at ${this.brokerURI}`);

          this.locationProvider = this.config.locationProvider;
          this.port = port;
          this.address = address;
          this.locationUpdateHandler = setInterval(async () => {
            let location = this.locationProvider();

            this.broker.publicPosition(location.latitude, location.longitude);
          }, 30000);

          resolve();
        } else {
          error(
            `LAMqttBrokerServer could not connect to broker at ${this.brokerURI}`
          );
          reject();
        }
      }
    });
  }

  public async stop(): Promise<void> {
    if (this.broker !== undefined) {
      await this.broker.disconnect();
      if (this.locationUpdateHandler) {
        clearInterval(this.locationUpdateHandler);
      }
    }
  }

  public getPort(): number {
    return this.port;
  }

  public getAddress(): string {
    return this.address;
  }
}
