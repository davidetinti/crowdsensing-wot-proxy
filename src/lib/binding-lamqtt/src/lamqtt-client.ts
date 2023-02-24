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
import { LAMqttClientConfig, LAMqttForm, LAMqttQoS } from "./lamqtt";
import { IPublishPacket, QoS } from "mqtt";
import * as url from "url";
import { Subscription } from "rxjs/Subscription";
import { Readable } from "stream";
import { MQTTMessage, MQTTReceiver, SpatialMQTTClient } from "la-mqtt";

const { debug, warn } = createLoggers("binding-lamqtt", "lamqtt-client");

declare interface LAMqttClientSecurityParameters {
  username: string;
  password: string;
}

export default class LAMqttClient implements ProtocolClient, MQTTReceiver {
  private scheme: string;

  private client: SpatialMQTTClient = undefined;

  private responses: Map<string, Array<string>>;

  constructor(private config: LAMqttClientConfig = {}, secure = false) {
    this.scheme = "mqtt" + (secure ? "s" : "");
  }

  messageRecv(message: MQTTMessage) {
    this.responses.get(message.topic).push(message.getContent());
  }

  public readResource(form: LAMqttForm): Promise<Content> {
    console.log("LAMQTT read resource");

    return new Promise<Content>((resolve, reject) => {
      // MQTT-based metadata
      const contentType = form.contentType;
      const requestUri = new url.URL(form.href);
      const askTopic = requestUri.pathname.slice(1);
      const answerTopic = form["lamqv:answerTopic"] as string;
      const brokerUri: string = `${this.scheme}://` + requestUri.host;
      const latitude = form["lamqv:latitude"] as number;
      const longitude = form["lamqv:longitude"] as number;
      const radius = form["lamqv:radius"] as number;
      const id = this.config.id;

      if (!id) reject("Missing id");
      if (!latitude || !longitude || !radius) reject("Missing location");

      this.initClient(brokerUri) // Ensure the client is connected and ready
        .then(
          () => this.client.subscribe(answerTopic),
          () => reject("Error connecting with broker")
        )
        .then(
          () => {
            this.responses.set(answerTopic, new Array<string>());
            return this.client.publicGeofence(
              latitude,
              longitude,
              radius,
              askTopic,
              "",
              id
            );
          },
          () => reject("Error during request")
        )
        .then(() => this.waitForTopic(answerTopic, contentType)) // Wait for a read response
        .then(
          (resource) => resolve(resource),
          (reason) => reject(reason)
        );
    });
  }

  public async writeResource(
    form: LAMqttForm,
    content: Content
  ): Promise<void> {
    throw new Error("Method not implemented.");
  }

  public invokeResource(form: LAMqttForm, content: Content): Promise<Content> {
    return new Promise<Content>((resolve, reject) => {
      // MQTT-based metadata
      const contentType = form.contentType;
      const requestUri = new url.URL(form.href);
      const askTopic = requestUri.pathname.slice(1);
      const answerTopic = form["lamqv:answerTopic"] as string;
      const brokerUri: string = `${this.scheme}://` + requestUri.host;
      const latitude = form["lamqv:latitude"] as number;
      const longitude = form["lamqv:longitude"] as number;
      const radius = form["lamqv:radius"] as number;
      const id = this.config.id;

      this.initClient(brokerUri)
        .then(
          () => this.client.subscribe(answerTopic),
          () => reject("Error connecting with broker")
        )
        .then(
          async () => {
            this.responses.set(answerTopic, new Array<string>());
            let message = "";
            if (content) {
              const data = await content.toBuffer();
              message = data.toString();
            }
            return this.client.publicGeofence(
              latitude,
              longitude,
              radius,
              askTopic,
              message,
              id
            );
          },
          () => reject("Error during request")
        )
        .then(() => this.waitForTopic(answerTopic, contentType)) // Wait for a read response
        .then(
          (resource) => resolve(resource),
          (reason) => reject(reason)
        );
    });
  }

  public async unlinkResource(form: TD.Form): Promise<void> {
    const requestUri = new url.URL(form.href);
    const topic = requestUri.pathname.slice(1);

    return new Promise<void>((resolve, reject) => {
      // TODO
      /*
      if (this.client && this.client.connected) {
        this.client.unsubscribe(topic);
        debug(`MqttClient unsubscribed from topic '${topic}'`);
      }
      */
      resolve();
    });
  }

  public subscribeResource(
    form: LAMqttForm,
    next: (value: Content) => void,
    error?: (error: Error) => void,
    complete?: () => void
  ): Promise<Subscription> {
    return new Promise<Subscription>((resolve, reject) => {
      // get MQTT-based metadata
      /*
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
      */
      resolve(new Subscription());
    });
  }

  public async start(): Promise<void> {
    // do nothing
  }

  public async stop(): Promise<void> {
    if (this.client) await this.client.disconnect();
  }

  public setSecurity(
    metadata: Array<TD.SecurityScheme>,
    credentials?: LAMqttClientSecurityParameters
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

  /**
   * Wait, at 1 sec interval, for a message corresponding to topic.
   * If the message doesn't arrive before 10 seconds it reject.
   * @param topic
   * @returns
   */
  private waitForTopic(topic: string, contentType: string): Promise<Content> {
    return new Promise<Content>((resolve, reject) => {
      const max_wait = 10;
      let wait = 0;
      const interval = setInterval(() => {
        wait++;
        if (this.responses.get(topic).length > 0) {
          clearInterval(interval);
          const message = this.responses.get(topic).shift();
          resolve(new Content(contentType, Readable.from(message)));
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
  private initClient(brokerUri: string): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      if (this.client === undefined) {
        const parsed = new url.URL(brokerUri);
        const address = parsed.href;
        const port = parseInt(parsed.port);
        const clientId = "TODO";
        this.client = new SpatialMQTTClient(
          this.config.username,
          this.config.password,
          address,
          port,
          clientId
        );
        this.client.connect().then(
          () => {
            this.responses = new Map<string, Array<string>>();
            this.client.setCallback(this);
            resolve();
          },
          () => {
            this.client = undefined;
            reject();
          }
        );
      } else {
        resolve();
      }
    });
  }
}
