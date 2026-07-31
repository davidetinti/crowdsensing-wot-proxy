// Allow use of .env files
require("dotenv").config();

import {
  MongoClient,
  Db,
  ObjectId,
  Document,
  WithId,
  FindOptions,
} from "mongodb";
import { ConsumedThing, ExposedThingInit } from "wot-typescript-definitions";
const dbName = process.env.MONGO_DB_NAME;
const dbUrl = process.env.MONGO_DB_URL;

/**
 * Code used to connect with MongoClient and retrieve a client instance
 */
export async function getDatabaseClient(): Promise<Db> {
  try {
    let client = await new MongoClient(dbUrl).connect();
    return client.db(dbName);
  } catch (error) {
    throw "Error connecting with database";
  }
}

export async function getItemWithID(
  db: Db,
  collectionName: string,
  id: string,
  options?: FindOptions<Document>
): Promise<WithId<Document>> {
  let collection = db.collection(collectionName);
  let item = await collection.findOne({ _id: new ObjectId(id) }, options);
  if (item) return item;
  else throw "Item not found in " + collectionName;
}

export class TimerTimeout {
  private timerObj: NodeJS.Timeout;

  public constructor(private fn: Function, private time: number) {
    this.timerObj = setTimeout(() => {
      this.fn();
      this.timerObj = null;
    }, this.time);
  }

  public isExpired(): boolean {
    return this.timerObj == null;
  }

  public stop(): TimerTimeout {
    if (this.timerObj != null) {
      clearTimeout(this.timerObj);
      this.timerObj = null;
    }
    return this;
  }

  // start timer using current settings (if it's not already running)
  public start(): TimerTimeout {
    if (!this.timerObj) {
      this.stop();
      this.timerObj = setTimeout(() => {
        this.fn();
        this.timerObj = null;
      }, this.time);
    }
    return this;
  }

  // start with new or original interval, stop current interval
  public reset(newTime: number = this.time): TimerTimeout {
    this.time = newTime;
    return this.stop().start();
  }
}

export class TimerInterval {
  private timerObj: number;

  public constructor(private fn: Function, private time: number) {
    this.timerObj = setInterval(this.fn, this.time);
  }

  public stop(): TimerInterval {
    if (this.timerObj) {
      clearInterval(this.timerObj);
      this.timerObj = null;
    }
    return this;
  }

  // start timer using current settings (if it's not already running)
  public start(): TimerInterval {
    if (!this.timerObj) {
      this.stop();
      this.timerObj = setInterval(this.fn, this.time);
    }
    return this;
  }

  // start with new or original interval, stop current interval
  public reset(newTime: number = this.time): TimerInterval {
    this.time = newTime;
    return this.stop().start();
  }
}

export declare type DeviceMap = {
  device: ConsumedThing;
  pinger: TimerTimeout;
  subscriptions: Map<string, TimerInterval | null>;
};

export declare type Submission = {
  _id: ObjectId;
  bearer: ObjectId;
  campaign: ObjectId;
  device: String;
  data: Object;
  time: Date;
};

export declare type User = {
  _id: ObjectId;
  email: String;
  salt: String;
  hash: String;
  points: Number;
  completed_campaigns: Array<ObjectId>;
};

export declare type Campaign = {
  _id: ObjectId;
  title: String;
  description: String;
  organization: String;
  ideal_submission_interval: Number;
  points: Number;
  pull_enabled: Boolean;
  push_auto_enabled: Boolean;
  push_input_enabled: Boolean;
  submission_required: Number;
  type: String;
  // Optional data schema (subset of JSON Schema / WoT DataSchema) describing
  // the payload devices must submit for this campaign. When present, the
  // proxy validates every submission (push and pull) against it. This is
  // what makes campaign types extensible: a new data type is defined by
  // inserting a campaign with its own schema, no code changes needed.
  data_schema?: DataSchema;
};

export declare type DataSchema = {
  type?: String;
  properties?: { [k: string]: DataSchemaProperty };
  required?: Array<String>;
};

export declare type DataSchemaProperty = {
  type?: String;
  title?: String;
  description?: String;
  unit?: String;
  minimum?: Number;
  maximum?: Number;
  enum?: Array<any>;
};

/**
 * Validate a submission payload against a campaign data schema.
 * Supports the subset of JSON Schema used by campaign definitions:
 * object type, per-property primitive types (number/integer/string/boolean),
 * required fields, minimum/maximum and enum constraints.
 * Returns null when the payload is valid, or a human readable error.
 */
export function validateAgainstSchema(
  data: any,
  schema: DataSchema | undefined
): string | null {
  if (!schema) return null;
  if ((schema.type ?? "object") !== "object")
    // Fail closed on misconfigured schemas instead of accepting anything.
    return "Unsupported schema type '" + schema.type + "'";
  if (typeof data !== "object" || data === null || Array.isArray(data))
    return "Payload must be an object";
  const properties = schema.properties ?? {};
  for (const name of schema.required ?? []) {
    if (data[String(name)] === undefined || data[String(name)] === null)
      return "Missing required field '" + name + "'";
  }
  for (const name in properties) {
    const value = data[name];
    if (value === undefined || value === null) continue;
    const error = validateValue(value, properties[name], name);
    if (error) return error;
  }
  return null;
}

function validateValue(
  value: any,
  property: DataSchemaProperty,
  name: string
): string | null {
  switch (property.type) {
    case "number":
    case "integer":
      if (typeof value !== "number")
        return "Field '" + name + "' must be a number";
      if (property.type === "integer" && !Number.isInteger(value))
        return "Field '" + name + "' must be an integer";
      if (
        property.minimum !== undefined &&
        value < property.minimum.valueOf()
      )
        return "Field '" + name + "' is below minimum " + property.minimum;
      if (
        property.maximum !== undefined &&
        value > property.maximum.valueOf()
      )
        return "Field '" + name + "' is above maximum " + property.maximum;
      break;
    case "string":
      if (typeof value !== "string")
        return "Field '" + name + "' must be a string";
      break;
    case "boolean":
      if (typeof value !== "boolean")
        return "Field '" + name + "' must be a boolean";
      break;
    default:
      break;
  }
  if (property.enum && !property.enum.some((allowed) => allowed === value))
    return "Field '" + name + "' must be one of " + JSON.stringify(property.enum);
  return null;
}

export const dashboardThingDescription: ExposedThingInit = {
  id: "crwsns:dashboard",
  title: "Dashboard",
  "cov:omit": true,
  properties: {
    campaigns: {
      type: "array",
      observable: false,
      readOnly: true,
    },
  },
  actions: {
    registerNewUser: {
      input: {
        type: "object",
        properties: {
          email: { type: "string" },
          password: { type: "string" },
        },
        required: ["email", "password"],
      },
      output: {
        type: "object",
        properties: {
          authenticated: { type: "boolean" },
          bearer: { type: "string" },
          error: { type: "string" },
        },
      },
      idempotent: false,
    },
    loginUser: {
      input: {
        type: "object",
        properties: {
          email: { type: "string" },
          password: { type: "string" },
        },
        required: ["email", "password"],
      },
      output: {
        type: "object",
        properties: {
          authenticated: { type: "boolean" },
          bearer: { type: "string" },
          error: { type: "string" },
        },
      },
      idempotent: false,
    },
    retrieveUserData: {
      input: {
        type: "object",
        properties: {
          bearer: { type: "string" }, //In this stage is used the User Object ID
        },
        required: ["bearer"],
      },
      output: {
        type: "object",
        properties: {
          email: { type: "string" },
          points: { type: "number" },
          campaigns: { type: "array" },
        },
      },
      idempotent: false,
    },
    applyToCampaignPull: {
      input: {
        type: "object",
        properties: {
          bearer: { type: "string" },
          campaignId: { type: "string" },
          deviceId: { type: "string" },
        },
        required: ["bearer", "campaignId", "deviceId"],
      },
      output: {
        type: "object",
        properties: {
          ok: { type: "boolean" },
          error: { type: "string" },
        },
      },
      idempotent: false,
    },
    applyToCampaignPush: {
      input: {
        type: "object",
        properties: {
          bearer: { type: "string" },
          campaignId: { type: "string" },
          deviceId: { type: "string" },
        },
        required: ["bearer", "campaignId", "deviceId"],
      },
      output: {
        type: "object",
        properties: {
          ok: { type: "boolean" },
          error: { type: "string" },
        },
      },
      idempotent: false,
    },
    sendPushCampaignData: {
      input: {
        type: "object",
        properties: {
          bearer: { type: "string" },
          campaignId: { type: "string" },
          deviceId: { type: "string" },
          data: { type: "object" },
        },
        required: ["bearer", "campaignId", "deviceId", "data"],
      },
      output: {
        type: "object",
        properties: {
          ok: { type: "boolean" },
          remaining: { type: "integer" },
          error: { type: "string" },
        },
      },
      idempotent: false,
    },
    ping: {
      input: {
        type: "object",
        properties: {
          deviceId: { type: "string" },
        },
        required: ["deviceId"],
      },
    },
    leaveCampaign: {
      input: {
        type: "object",
        properties: {
          bearer: { type: "string" },
          campaignId: { type: "string" },
          deviceId: { type: "string" },
        },
        required: ["bearer", "campaignId", "deviceId"],
      },
      output: {
        type: "object",
        properties: {
          ok: { type: "boolean" },
          error: { type: "string" },
        },
      },
      idempotent: false,
    },
  },
  description: "A basic thing to interact with the campaigns",
};
