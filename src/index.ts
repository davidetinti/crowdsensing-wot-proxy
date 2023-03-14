import { HttpServer } from "@node-wot/binding-http";
import { MqttClientFactory } from "./lib/binding-mqtt";
import { LAMqttClientFactory } from "./lib/binding-lamqtt";
import {
  Campaign,
  dashboardThingDescription,
  DeviceMap,
  getDatabaseClient,
  getItemWithID,
  TimerInterval,
  TimerTimeout,
  User,
} from "./utils";
import Servient from "@node-wot/core";
import { Db } from "mongodb";
import {
  ExposedThing,
  InteractionInput,
  InteractionOutput,
  ThingDescription,
} from "wot-typescript-definitions";
import fetch from "node-fetch";
import * as crypto from "crypto";
import { LAMqttBrokerServer } from "./lib/binding-lamqtt/src/lamqtt";

// Allow use of .env files
require("dotenv").config();

const baseUri = process.env.BASE_HTTP_URI;
const TDD_URL = process.env.TDD_URL;
const DEVICE_OFFLINE_TIMEOUT = 20000;

let db: Db;
let wot: typeof WoT;
let servient = new Servient();
let deviceMapper: { [k: string]: DeviceMap } = {};

// Start routine for dashboard servient
servient.addClientFactory(new MqttClientFactory());
//servient.addClientFactory(new LAMqttClientFactory());
//servient.addServer(new CoapServer());
//servient.addServer(new CoapServer(8080, "192.168.178.26"));
//servient.addServer(new HttpServer());
/*
servient.addServer(
  new LAMqttBrokerServer({
    uri: "mqtt://vz5nc4zqbrgzpn13.myfritz.net:1883",
    clientId: "testClientID",
    locationProvider: () => {
      return { latitude: null, longitude: null };
    },
  })
);
*/
servient.addServer(new HttpServer({ baseUri: baseUri }));

console.log("Starting servient...");
servient.start().then(async (_wot) => {
  wot = _wot;
  db = await getDatabaseClient();
  let dashboardThing = await wot.produce(dashboardThingDescription);
  setHandlers(dashboardThing);

  console.log("Exposing thing...");
  await dashboardThing.expose();
  console.log("Thing exposed.");

  registerThing(dashboardThing);
  subscribeToUpdates();
});

/**
 * Set dashboard handlers.
 */
function setHandlers(thing: ExposedThing): void {
  console.log("Setting handlers...");
  thing.setPropertyReadHandler("campaigns", campaignsReadHandler);
  thing.setActionHandler("registerNewUser", registerNewUserHandler);
  thing.setActionHandler("loginUser", loginUserHandler);
  thing.setActionHandler("retrieveUserData", retrieveUserDataHandler);
  thing.setActionHandler("applyToCampaignPull", applyToCampaignPullHandler);
  thing.setActionHandler("applyToCampaignPush", applyToCampaignPushHandler);
  thing.setActionHandler("sendPushCampaignData", sendPushCampaignDataHandler);
  thing.setActionHandler("ping", pingHandler);
  thing.setActionHandler("leaveCampaign", leaveCampaignHandler);
  console.log("Handlers setted.");
}

/**
 * Read campaigns property.
 */
async function campaignsReadHandler(): Promise<undefined | InteractionInput> {
  try {
    let result = await db.collection("campaigns").find().toArray();
    return result;
  } catch (error) {
    console.log(error);
    return [];
  }
}

type AuthorizedRequest = {
  bearer: string;
};

type AccessRequest = {
  email: string;
  password: string;
};

type JoinRequest = AuthorizedRequest & {
  campaignId: string;
  deviceId: string;
};

type LeaveRequest = AuthorizedRequest & {
  campaignId: string;
  deviceId: string;
};

type SubmissionRequest = AuthorizedRequest & {
  campaignId: string;
  deviceId: string;
  data: object;
};

type PingRequest = {
  deviceId: string;
};

/**
 * Add a new user to database and return its ID used as bearer token.
 */
async function registerNewUserHandler(
  parameters: InteractionOutput
): Promise<undefined | InteractionInput> {
  try {
    let data = (await parameters.value()) as AccessRequest;
    let users = db.collection("users");
    let existingUser = await users.findOne({ email: data.email });
    if (existingUser) throw "User already registered";
    else {
      let newUser = {
        email: data.email,
        points: 0,
        completed_campaigns: [],
        salt: "",
        hash: "",
      };
      newUser.salt = crypto.randomBytes(16).toString("hex");
      newUser.hash = crypto
        .pbkdf2Sync(data.password, newUser.salt, 1000, 64, `sha512`)
        .toString(`hex`);
      let result = await users.insertOne(newUser);
      if (result.acknowledged) {
        return {
          authenticated: true,
          bearer: String(result.insertedId),
        };
      } else {
        throw "Error inserting element";
      }
    }
  } catch (error) {
    console.log(error);
    return {
      authenticated: false,
      error: error,
    };
  }
}

/**
 * Return the ID used as bearer token of an exisitng user.
 */
async function loginUserHandler(
  parameters: InteractionOutput
): Promise<undefined | InteractionInput> {
  try {
    let data = (await parameters.value()) as AccessRequest;
    let users = db.collection("users");
    let user = await users.findOne({
      email: data.email,
    });
    if (user) {
      let hash = crypto
        .pbkdf2Sync(data.password, user.salt, 1000, 64, `sha512`)
        .toString(`hex`);
      if (user.hash != hash) throw "Incorrect password";
      else
        return {
          authenticated: true,
          bearer: String(user._id),
        };
    } else throw "Incorrect email";
  } catch (error) {
    console.log(error);
    return {
      authenticated: false,
      error: error,
    };
  }
}

/**
 * Return user data associated with a bearer token.
 */
async function retrieveUserDataHandler(
  parameters: InteractionOutput
): Promise<undefined | InteractionInput> {
  try {
    let data = (await parameters.value()) as AuthorizedRequest;
    let options = {
      projection: {
        email: 1,
        points: 1,
        completed_campaigns: 1,
      },
    };
    let user = (await getItemWithID(db, "users", data.bearer, options)) as User;
    return user;
  } catch (error) {
    console.log(error);
    return null;
  }
}

/**
 * Request from a device to apply to a campaign sending data automatically (pull).
 */
async function applyToCampaignPullHandler(
  parameters: InteractionOutput
): Promise<undefined | InteractionInput> {
  try {
    let data = (await parameters.value()) as JoinRequest;
    let user = (await getItemWithID(db, "users", data.bearer)) as User;
    let campaign = (await getItemWithID(
      db,
      "campaigns",
      data.campaignId
    )) as Campaign;
    if (
      !user.completed_campaigns
        .map((id) => id.toString())
        .includes(campaign._id.toString())
    ) {
      let deviceMap = deviceMapper[data.deviceId];
      if (deviceMap == undefined) throw "Device not found";
      startPullRoutine(deviceMap, campaign, user);
      return {
        ok: true,
        error: null,
      };
    } else throw "User already completed this campaign";
  } catch (error) {
    console.log(error);
    return {
      ok: false,
      error: error,
    };
  }
}

/**
 */
function startPullRoutine(
  deviceMap: DeviceMap,
  campaign: Campaign,
  user: User
) {
  deviceMap.subscriptions.set(
    campaign._id.toString(),
    new TimerInterval(async () => {
      if (!deviceMap.pinger.isExpired()) {
        try {
          let reading = await deviceMap.device.readProperty(
            campaign._id.toString()
          );
          let data = await reading.value();
          switch (campaign.type) {
            // check that data includes required fields, i.e. data are valid
            case "location":
              break;

            default:
              break;
          }
          // Save sent data
          let insertResult = await db.collection("submissions").insertOne({
            bearer: user._id,
            campaignId: campaign._id,
            device: deviceMap.device.getThingDescription().id,
            data: data,
            time: new Date(),
          });
          if (insertResult.acknowledged) {
            console.log("Successfully collected data!");
            // Data sent correctly saved
            let submissionCount = await db
              .collection("submissions")
              .countDocuments({
                bearer: user._id,
                campaignId: campaign._id,
                device: deviceMap.device.getThingDescription().id,
              });
            if (submissionCount >= campaign.submission_required) {
              // Campaign completed
              let campaignRoutine = deviceMap.subscriptions.get(
                campaign._id.toString()
              );
              if (campaignRoutine != null) campaignRoutine.stop();
              deviceMap.subscriptions.delete(campaign._id.toString());
              // TODO: calculate points and add to user, now it add full points
              let updateResult = await db.collection("users").updateOne(
                { _id: user._id },
                {
                  $push: { completed_campaigns: campaign._id },
                  $inc: { points: campaign.points },
                }
              );
              if (!updateResult.acknowledged)
                console.log("Error completing campaign");
            }
          } else {
            console.log("Error saving data");
          }
        } catch (error) {
          console.log(error);
        }
      } else console.log("Called but device was offline");
    }, campaign.ideal_submission_interval.valueOf() * 1000)
  );
}

/**
 * Request from a device to apply to a campaign sending data manually (push).
 */
async function applyToCampaignPushHandler(
  parameters: InteractionOutput
): Promise<undefined | InteractionInput> {
  try {
    let data = (await parameters.value()) as JoinRequest;
    let user = (await getItemWithID(db, "users", data.bearer)) as User;
    let campaign = (await getItemWithID(
      db,
      "campaigns",
      data.campaignId
    )) as Campaign;
    if (
      !user.completed_campaigns
        .map((id) => id.toString())
        .includes(campaign._id.toString())
    ) {
      let deviceMap = deviceMapper[data.deviceId];
      if (deviceMap == undefined) throw "Device not found";
      deviceMap.subscriptions.set(campaign._id.toString(), null);
      return {
        ok: true,
        error: null,
      };
    } else throw "User already completed this campaign";
  } catch (error) {
    console.log(error);
    return {
      ok: false,
      error: error,
    };
  }
}

/**
 * Receive a manual data submission from a device, and handle its storing process.
 */
async function sendPushCampaignDataHandler(
  parameters: InteractionOutput
): Promise<undefined | InteractionInput> {
  try {
    let data = (await parameters.value()) as SubmissionRequest;
    let user = (await getItemWithID(db, "users", data.bearer)) as User;
    let campaign = (await getItemWithID(
      db,
      "campaigns",
      data.campaignId
    )) as Campaign;
    // Check if user already completed the campaign
    if (
      user.completed_campaigns
        .map((id) => id.toString())
        .includes(campaign._id.toString())
    )
      throw "User already completed this campaign";
    let deviceMap = deviceMapper[data.deviceId];
    if (deviceMap == undefined) throw "Device not found";
    // Check if user joined the campaign
    if (!deviceMap.subscriptions.has(campaign._id.toString()))
      throw "User not applied to this campaign";
    // Save sent data
    let insertResult = await db.collection("submissions").insertOne({
      bearer: user._id,
      campaignId: campaign._id,
      device: deviceMap.device.getThingDescription().id,
      data: data.data,
      time: new Date(),
    });
    if (insertResult.acknowledged) {
      console.log("Successfully collected data!");
      // Data sent correctly saved
      let submissionCount = await db.collection("submissions").countDocuments({
        bearer: user._id,
        campaignId: campaign._id,
        device: deviceMap.device.getThingDescription().id,
      });
      if (submissionCount < campaign.submission_required) {
        // Campaign not already completed
        return {
          ok: true,
          remaining: campaign.submission_required.valueOf() - submissionCount,
        };
      } else {
        // Campaign completed
        deviceMap.subscriptions.delete(campaign._id.toString());
        // TODO: calculate points and add to user, now it add full points
        let updateResult = await db.collection("users").updateOne(
          { _id: user._id },
          {
            $push: { completed_campaigns: campaign._id },
            $inc: { points: campaign.points },
          }
        );
        if (updateResult.acknowledged) {
          return {
            ok: true,
            remaining: 0,
          };
        } else throw "Error completing campaign";
      }
    } else {
      throw "Error saving data";
    }
  } catch (error) {
    console.log(error);
    return {
      ok: false,
      error: error,
    };
  }
}

/**
 * Restart the pinger timeout of a device.
 */
async function pingHandler(
  parameters: InteractionOutput
): Promise<undefined | InteractionInput> {
  try {
    let deviceId = ((await parameters.value()) as PingRequest).deviceId;
    let deviceMap = deviceMapper[deviceId];
    if (deviceMap) {
      deviceMap.pinger.reset();
      console.log("Resetted timeout");
    } else {
      thingCreatedHandler({ id: deviceId });
    }
  } catch (error) {
    console.log(error);
  }
  return;
}

/**
 * Remove the campaign subscription from the device mapper.
 */
async function leaveCampaignHandler(
  parameters: InteractionOutput
): Promise<undefined | InteractionInput> {
  try {
    let data = (await parameters.value()) as LeaveRequest;
    let deviceMap = deviceMapper[data.deviceId];
    if (deviceMap == undefined) throw "Device not found";
    let campaignRoutine = deviceMap.subscriptions.get(data.campaignId);
    if (campaignRoutine != null) campaignRoutine.stop();
    deviceMap.subscriptions.delete(data.campaignId);
    return {
      ok: true,
      error: null,
    };
  } catch (error) {
    console.log(error);
    return {
      ok: false,
      error: error,
    };
  }
}

/**
 * Register a TD on a TDD
 */
async function registerThing(thing: ExposedThing) {
  try {
    console.log("Registering thing...");
    let response = await fetch(
      TDD_URL + "/things/" + thing.getThingDescription().id,
      {
        body: JSON.stringify(thing),
        method: "PUT",
      }
    );
    if (response.ok) {
      console.log("Thing registered.");
    } else {
      throw await response.json();
    }
  } catch (error) {
    console.log("Unable to register TD: " + error);
    console.log("Retrying in 5 seconds...");
    setTimeout(() => registerThing(thing), 5000);
  }
}

async function subscribeToUpdates() {
  try {
    console.log("Subscribing to updates from TDD...");
    let response = await fetch(TDD_URL + "/events?diff=true");
    let stream = response.body;
    stream.setEncoding("utf-8");
    stream.on("readable", () => {
      stream.read();
    });
    stream.on("data", (chunk) => {
      let splitted = chunk.slice(0, chunk.length - 2).split("\n");
      let event = splitted[0].split(":", 2)[1].trim();
      let data = JSON.parse(splitted[2].slice(6, splitted[2].length));
      if (data.id.includes("smartphone_")) {
        switch (event) {
          case "thing_created":
            thingCreatedHandler(data);
            break;
          case "thing_updated":
            thingUpdatedHandler(data);
            break;
          case "thing_deleted":
            thingDeletedHandler(data);
            break;
          default:
            break;
        }
      }
    });
  } catch (error) {
    console.log("Unable to subscribe to updates: " + error);
    console.log("Retrying in 5 seconds...");
    setTimeout(() => subscribeToUpdates(), 5000);
  }
}

/**
 * Function used to restore the subscription routines of a device.
 * Assumes that there are no active routines associated with the device.
 * @param {ConsumedThing} device
 */
function restoreSubscriptions(device) {
  /** @type {Map<String, TimerInterval>} */
  let subscriptions = new Map();
  let properties = device.getThingDescription().properties;
  for (const key in properties) {
    if (Object.hasOwnProperty.call(properties, key)) {
      /** @type {import("wot-thing-description-types").PropertyElement} */
      const property = properties[key];
    }
  }
  device.getThingDescription().properties;
  // TODO: Restore already joined campaign subscription routines
  return subscriptions;
}

/**
 * Function called when a TD on the TDD is created (usually when a new user register
 * or when an existing user login).
 * If the user is new subscriptions will be empty otherwise it'll be filled with all the
 * previously joined campaigns, found via exposed paths on the consumed device.
 * Assumes the device is not already added to the dashboard.
 * @param {Object} data
 * @param {String} data.id
 */
async function thingCreatedHandler(data) {
  let response = await fetch(TDD_URL + "/things/" + data.id);
  let consumedDevice = await wot.consume(
    (await response.json()) as ThingDescription
  );
  let consumedDeviceId = consumedDevice.getThingDescription().id;
  deviceMapper[consumedDeviceId] = {
    device: consumedDevice,
    pinger: new TimerTimeout(() => {
      console.log("Device expired");
    }, DEVICE_OFFLINE_TIMEOUT),
    subscriptions: restoreSubscriptions(consumedDevice),
  };
  console.log("Device setted");
  console.log("Thing " + consumedDeviceId + " created");
}

/**
 * Function called when a TD on the TDD is updated (usually its called when a previously
 * connected device came back online)
 * Restart the entry of the device updated, or create a new if not present.
 * TODO: Detect change on subscription
 * @param {Object} data
 * @param {String} data.id
 */
async function thingUpdatedHandler(data) {
  let response = await fetch(TDD_URL + "/things/" + data.id);
  let consumedDevice = await wot.consume(
    (await response.json()) as ThingDescription
  );
  let consumedDeviceId = consumedDevice.getThingDescription().id;
  if (deviceMapper[consumedDeviceId]) {
    let deviceMap = deviceMapper[consumedDeviceId];
    deviceMap.pinger.reset();
    deviceMap.device = consumedDevice;
    console.log("Device resetted");
  } else {
    deviceMapper[consumedDeviceId] = {
      device: consumedDevice,
      pinger: new TimerTimeout(() => {
        console.log("Device expired");
      }, DEVICE_OFFLINE_TIMEOUT),
      subscriptions: restoreSubscriptions(consumedDevice),
    };
    console.log("Device setted");
  }
  console.log("Thing " + consumedDeviceId + " updated");
}

/**
 * Function called when a TD on the TDD is deleted (usually its called when a
 * user logout from the application).
 * Stops all the routine associated with the device and remove any reference to it.
 * @param {Object} data
 * @param {String} data.id
 */
function thingDeletedHandler(data) {
  if (deviceMapper[data.id]) {
    let deviceMap = deviceMapper[data.id];
    if (deviceMap != undefined) {
      deviceMap.pinger.stop();
      deviceMap.subscriptions.forEach((subscription) => subscription.stop());
      delete deviceMapper[data.id];
    }
  }
  console.log("Thing " + data.id + " deleted");
}
