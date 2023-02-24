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
 * Protocol test suite to test protocol implementations
 */

import {
  ProtocolClient,
  Content,
  ContentSerdes,
  ProtocolHelpers,
  createLoggers,
} from "@node-wot/core";
import * as TD from "@node-wot/td-tools";
import * as mqtt from "mqtt";
import { MqttClientConfig, MqttForm, MqttQoS } from "./mqtt";
import { IPublishPacket, QoS } from "mqtt";
import * as url from "url";
import { Subscription } from "rxjs/Subscription";
import { Readable } from "stream";

const { debug, warn } = createLoggers("binding-mqtt", "mqtt-client");

declare interface MqttClientSecurityParameters {
  username: string;
  password: string;
}

export default class MqttClient implements ProtocolClient {
  private scheme: string;

  constructor(private config: MqttClientConfig = {}, secure = false) {
    this.scheme = "mqtt" + (secure ? "s" : "");
  }

  private client: mqtt.MqttClient = undefined;

  private responses: Map<string, Array<Content>>;

  public readResource(form: MqttForm): Promise<Content> {
    console.log("mqtt read resource");

    return new Promise<Content>((resolve, reject) => {
      // MQTT-based metadata
      const contentType = form.contentType;
      const requestUri = new url.URL(form.href);
      const askTopic = requestUri.pathname.slice(1);
      const answerTopic = form["mqv:answerTopic"];
      const brokerUri: string = `${this.scheme}://` + requestUri.host;

      this.verifyConnection(brokerUri) // Ensure the client is connected and ready
        .then(() => {
          this.verifyMessageListener(contentType); // Ensure the listener for incoming messages is setted
          return new Promise<void>((resolve, reject) => {
            // Read resource routine
            this.client.subscribe(answerTopic, (err, granted) => {
              if (err === null) {
                this.responses.set(answerTopic, new Array<Content>());
                this.client.publish(askTopic, "", (err) => {
                  if (err === undefined) resolve();
                  else reject("Failed to publish to askTopic");
                });
              } else reject("Failed to subscribe to readTopic");
            });
          });
        })
        .then(() => this.waitForTopic(answerTopic)) // Wait for a read response
        .then(
          (resource) => resolve(resource),
          (reason) => reject(reason)
        );
    });
  }

  public async writeResource(form: MqttForm, content: Content): Promise<void> {
    throw new Error("Method not implemented.");
  }

  public invokeResource(form: MqttForm, content: Content): Promise<Content> {
    return new Promise<Content>((resolve, reject) => {
      // MQTT-based metadata
      const requestUri = new url.URL(form.href);
      const topic = requestUri.pathname.slice(1);
      const brokerUri = `${this.scheme}://${requestUri.host}`;

      this.verifyConnection(brokerUri).then(async () => {
        // If not input was provided, set up an own body otherwise take input as body
        // Default value setted as empty string because wot-android default value for action input type is "string"
        if (content === undefined) {
          this.client.publish(topic, "");
        } else {
          const buffer = await ProtocolHelpers.readStreamFully(content.body);
          this.client.publish(topic, buffer);
        }
        // there will bo no response
        resolve(new Content(ContentSerdes.DEFAULT, Readable.from([])));
      });
    });
  }

  public async unlinkResource(form: TD.Form): Promise<void> {
    const requestUri = new url.URL(form.href);
    const topic = requestUri.pathname.slice(1);

    return new Promise<void>((resolve, reject) => {
      if (this.client && this.client.connected) {
        this.client.unsubscribe(topic);
        debug(`MqttClient unsubscribed from topic '${topic}'`);
      }
      resolve();
    });
  }

  public subscribeResource(
    form: MqttForm,
    next: (value: Content) => void,
    error?: (error: Error) => void,
    complete?: () => void
  ): Promise<Subscription> {
    return new Promise<Subscription>((resolve, reject) => {
      // get MQTT-based metadata
      const contentType = form.contentType;
      const requestUri = new url.URL(form.href);
      const topic = requestUri.pathname.slice(1);
      const brokerUri: string = `${this.scheme}://` + requestUri.host;

      if (this.client === undefined) {
        this.client = mqtt.connect(brokerUri, this.config);
      }

      this.client.on("connect", () => {
        this.client.subscribe(topic);
        resolve(
          new Subscription(() => {
            this.client.unsubscribe(topic);
          })
        );
      });
      this.client.on(
        "message",
        (receivedTopic: string, payload: string, packet: IPublishPacket) => {
          debug(
            `Received MQTT message (topic: ${receivedTopic}, data: ${payload})`
          );
          if (receivedTopic === topic) {
            next(new Content(contentType, Readable.from(payload)));
          }
        }
      );
      this.client.on("error", (err: Error) => {
        if (this.client) {
          this.client.end();
        }
        this.client = undefined;
        // TODO: error handling
        if (error) error(err);

        reject(err);
      });
    });
  }

  public async start(): Promise<void> {
    // do nothing
  }

  public async stop(): Promise<void> {
    if (this.client) this.client.end();
  }

  public setSecurity(
    metadata: Array<TD.SecurityScheme>,
    credentials?: MqttClientSecurityParameters
  ): boolean {
    if (
      metadata === undefined ||
      !Array.isArray(metadata) ||
      metadata.length === 0
    ) {
      warn(`MqttClient received empty security metadata`);
      return false;
    }
    const security: TD.SecurityScheme = metadata[0];

    if (security.scheme === "basic") {
      this.config.username = credentials.username;
      this.config.password = credentials.password;
    }
    return true;
  }

  private mapQoS(qos: MqttQoS): QoS {
    switch (qos) {
      case 2:
        return (qos = 2);
      case 1:
        return (qos = 1);
      case 0:
      default:
        return (qos = 0);
    }
  }

  /**
   * Wait, at 1 sec interval, for a message corresponding to topic.
   * If the message doesn't arrive before 10 seconds it reject.
   * @param topic
   * @returns
   */
  private waitForTopic(topic: string): Promise<Content> {
    return new Promise<Content>((resolve, reject) => {
      let max_wait = 10;
      let wait = 0;
      let interval = setInterval(() => {
        wait++;
        if (this.responses.get(topic).length > 0) {
          clearInterval(interval);
          resolve(this.responses.get(topic).shift());
        }
        if (wait >= max_wait) {
          clearInterval(interval);
          reject("Waited too long for response");
        }
      }, 1000);
    });
  }

  /**
   * @param brokerUri
   * @returns Resolve if the connection is established and the client is ready
   *          Reject otherwise
   */
  private verifyConnection(brokerUri: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (this.client === undefined || this.client.disconnected) {
        this.client = mqtt.connect(brokerUri, this.config);
        this.client.once("connect", () => resolve());
        this.client.once("error", () => reject());
      } else if (this.client.connected) resolve();
      else if (this.client.disconnecting) {
        this.client.once("disconnect", () => {
          this.client = mqtt.connect(brokerUri, this.config);
          this.client.once("connect", () => resolve());
          this.client.once("error", () => reject());
        });
      } else if (this.client.reconnecting) {
        this.client.once("reconnect", () => resolve());
      } else reject();
    });
  }

  /**
   * Check message listener presence, and create it if necessary
   * @param contentType
   */
  private verifyMessageListener(contentType: string): void {
    if (this.responses === undefined)
      this.responses = new Map<string, Array<Content>>();
    if (this.client.listenerCount("message") == 0) {
      this.client.on(
        "message",
        (receivedTopic: string, payload: Buffer, packet: IPublishPacket) => {
          this.responses
            .get(receivedTopic)
            .push(new Content(contentType, Readable.from(payload)));
        }
      );
    }
  }
}
