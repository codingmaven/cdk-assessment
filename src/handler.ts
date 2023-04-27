import * as aws from "aws-sdk";
import { of } from "rxjs";

type SQSUser = {
    id: string;
    lid: string;
    data: {
        formId: string;
        firstName: string;
        lastName: string;
        emailAddress: string;
        userType: string;
        passcode?: number;
    }
}

const generatePassCode = () => {
    return Math.floor(10000000 + Math.random() * 90000000);
}

const handler = async function (event: any) {
    const tableName = process.env.TABLE_NAME! as string;
    const queueUrl = process.env.IDENTITY_LOOKUP_QUEUE! as string;

    if (!tableName || !queueUrl) {
        throw new Error('tableName and queueUrl are required!');
    }

    const db = new aws.DynamoDB.DocumentClient();
    const sqs = new aws.SQS();

    const adminList: SQSUser[] = [];
    const userList: SQSUser[] = [];

    // parse MSK events and get list of admin users and normal users
    for (const key in event.records) {
        // Iterate through records
        event.records[key].map((record: any) => {
            // Decode base64
            const msg = JSON.parse(Buffer.from(record.value, 'base64').toString());
            const user = msg.payload as SQSUser;
            if (user.data.userType !== 'admin') {
                user.data.passcode = generatePassCode();
                adminList.push(user);
            }
            else {
                userList.push(user);
            }
        });
    }

    // Process admin users and upload them to dynamodb
    const usersForDynamoDb: aws.DynamoDB.DocumentClient.WriteRequests = [];
    of(...adminList)
        .subscribe((user) => usersForDynamoDb.push({
            PutRequest: {
                Item: aws.DynamoDB.Converter.marshall(user)
            }
        }));
    if (usersForDynamoDb.length) {
        const dbParams: aws.DynamoDB.Types.BatchWriteItemInput = {
            RequestItems: {
                [tableName]: usersForDynamoDb
            }
        };
        await db.batchWrite(dbParams).promise();
    }

    // process normal users and send them to SQS
    const usersForSQS: aws.SQS.SendMessageBatchRequestEntryList = [];
    of(...userList)
        .subscribe((user) =>  usersForSQS.push({
            Id: user.id,
            MessageBody: JSON.stringify(user)
        }));

    if (usersForSQS.length) {
        const sqsParams: aws.SQS.Types.SendMessageBatchRequest = {
            Entries: usersForSQS,
            QueueUrl: queueUrl
        };

        await sqs.sendMessageBatch(sqsParams).promise();
    }
}

export {
    handler,
    aws
}