const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, GetCommand } = require("@aws-sdk/lib-dynamodb");
const { ApiGatewayManagementApiClient, PostToConnectionCommand } = require("@aws-sdk/client-apigatewaymanagementapi");

const ddbClient = new DynamoDBClient({ region: "ap-south-1" });
const ddb = DynamoDBDocumentClient.from(ddbClient);

const TABLE_NAME = process.env.CONNECTIONS_TABLE || "vishnuram-chat-connections";

exports.handler = async (event) => {
  const domain = event.requestContext.domainName;
  const stage = event.requestContext.stage;
  const senderConnectionId = event.requestContext.connectionId;

  const apiGwClient = new ApiGatewayManagementApiClient({
    endpoint: `https://${domain}/${stage}`,
  });

  let body;
  try {
    body = JSON.parse(event.body);
  } catch (err) {
    await postTo(apiGwClient, senderConnectionId, { error: "Invalid JSON body" });
    return { statusCode: 400, body: "Invalid JSON" };
  }

  const { to, message } = body;
  // 'from' is taken from the sender's own connection record via a Scan,
  // since $connect stored userId <-> connectionId but this route only gets connectionId.
  const from = await lookupUserIdByConnection(senderConnectionId);

  if (!to || !message) {
    await postTo(apiGwClient, senderConnectionId, { error: "Both 'to' and 'message' are required" });
    return { statusCode: 400, body: "Missing 'to' or 'message'" };
  }

  // Look up recipient's current connection
  let recipient;
  try {
    const result = await ddb.send(
      new GetCommand({
        TableName: TABLE_NAME,
        Key: { userId: to },
      })
    );
    recipient = result.Item;
  } catch (err) {
    console.log("Failed to look up recipient", err);
    await postTo(apiGwClient, senderConnectionId, { error: "Internal error looking up recipient" });
    return { statusCode: 500, body: "Lookup failed" };
  }

  if (!recipient) {
    await postTo(apiGwClient, senderConnectionId, { error: `User '${to}' is not currently connected` });
    return { statusCode: 200, body: "Recipient offline" };
  }

  // Push message directly to recipient's connection - nothing is persisted
  try {
    await postTo(apiGwClient, recipient.connectionId, {
      from,
      message,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    if (err.name === "GoneException") {
      console.log(`Recipient connection ${recipient.connectionId} is stale`);
      await postTo(apiGwClient, senderConnectionId, { error: `User '${to}' connection expired` });
      return { statusCode: 200, body: "Recipient connection gone" };
    }
    console.log("Failed to deliver message", err);
    await postTo(apiGwClient, senderConnectionId, { error: "Failed to deliver message" });
    return { statusCode: 500, body: "Delivery failed" };
  }

  return { statusCode: 200, body: "Message sent" };
};

async function lookupUserIdByConnection(connectionId) {
  const { ScanCommand } = require("@aws-sdk/lib-dynamodb");
  try {
    const result = await ddb.send(
      new ScanCommand({
        TableName: TABLE_NAME,
        FilterExpression: "connectionId = :cid",
        ExpressionAttributeValues: { ":cid": connectionId },
      })
    );
    return result.Items && result.Items.length > 0 ? result.Items[0].userId : "unknown";
  } catch (err) {
    console.log("Failed to look up sender identity", err);
    return "unknown";
  }
}

async function postTo(client, connectionId, payload) {
  return client.send(
    new PostToConnectionCommand({
      ConnectionId: connectionId,
      Data: Buffer.from(JSON.stringify(payload)),
    })
  );
}
