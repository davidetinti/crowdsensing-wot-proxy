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

import { Form } from "@node-wot/td-tools";

export { default as LAMqttClient } from "./lamqtt-client";
export { default as LAMqttClientFactory } from "./lamqtt-client-factory";
export { default as LAMqttBrokerServer } from "./lamqtt-broker-server";

export * from "./lamqtt-client";
export * from "./lamqtt-client-factory";
export * from "./lamqtt-broker-server";

/**
 * MQTT Quality of Service level.
 * QoS0: Fire-and-forget
 * QoS1: Deliver-at-least-once
 * QoS2: Deliver-exactly-once
 */
export enum LAMqttQoS {
  QoS0,
  QoS1,
  QoS2,
}

export class LAMqttForm extends Form {
  public "lamqv:qos": LAMqttQoS = LAMqttQoS.QoS0;
  public "lamqv:retain": boolean;
  public "lamqv:answerTopic": string;
  public "lamqv:latitude": number;
  public "lamqv:longitude": number;
  public "lamqv:radius": number;
}

export interface LAMqttClientConfig {
  // username & password are redundated here (also find them in MqttClientSecurityParameters)
  // because MqttClient.setSecurity() method can inject authentication credentials into this interface
  // which will be then passed to mqtt.connect() once for all
  username?: string;
  password?: string;
  rejectUnauthorized?: boolean;
  id?: string;
}

export interface LAMqttBrokerServerConfig {
  uri: string;
  clientId: string;
  locationProvider: LAMqttLocationProvider;
}

export interface LAMqttLocationProvider {
  (): { latitude: number; longitude: number };
}
