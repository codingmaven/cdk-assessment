import * as awsMock from 'aws-sdk-mock';
import { aws, handler } from '../src/handler'

test('Should run successfully and send data correctly to dynamodb and sqs', async () => {
  process.env.TABLE_NAME = 'userTable';
  process.env.IDENTITY_LOOKUP_QUEUE = 'https://sqs.us-east-1.amazonaws.com/123456789012/userQueue';

  const eventExample = {
    records: {
      ['offset.1']: [{
        key: '1233456789',
        headers: [],
        offset: 1,
        partition: 1,
        timestamp: new Date().getTime(),
        topic: 'kafkaTopic',
        timestampType: 'CREATE_TIME',
        value: Buffer.from(JSON.stringify({
          eventType: 'FormDataProcessed',
          payload: {
            id: '123',
            lid: '123123',
            data: {
              formId: 'KM_HomeEdition_Short_155',
              firstName: 'Jane',
              lastName: 'Connor',
              emailAddress: 'example@email.com',
              userType: 'admin'
            }
          }
        })).toString('base64')
      }],
      ['offset.2']: [{
        key: '1233456788',
        headers: [],
        offset: 2,
        partition: 1,
        timestamp: new Date().getTime(),
        topic: 'kafkaTopic',
        timestampType: 'CREATE_TIME',
        value: Buffer.from(JSON.stringify({
          eventType: 'FormDataProcessed',
          payload: {
            id: '123',
            lid: '123123',
            data: {
              formId: 'KM_HomeEdition_Short_155',
              firstName: 'John',
              lastName: 'Connor',
              emailAddress: 'example@email2.com',
              userType: 'user'
            }
          }
        })).toString('base64')
      }]
    },
    eventSource: 'aws:kafka',
    eventSourceArn: ''
  }

  const batchWriteMock = jest.fn().mockResolvedValue({
    promise: () => Promise.resolve(true)
  });
  const sendMessageBatchMock = jest.fn().mockResolvedValue({
    promise: () => Promise.resolve(true)
  });


  awsMock.setSDKInstance(aws);

  awsMock.mock('DynamoDB.DocumentClient', 'batchWrite', batchWriteMock);
  awsMock.mock('SQS', 'sendMessageBatch', sendMessageBatchMock);

  await handler(eventExample);
  expect(batchWriteMock).toHaveBeenCalledTimes(1);
  expect(sendMessageBatchMock).toHaveBeenCalledTimes(1);

  expect(batchWriteMock).toHaveBeenCalledWith({
    "RequestItems": {
      "userTable": [
        {
          "PutRequest": {
            "Item": {
              "id": {
                "S": "123"
              },
              "lid": {
                "S": "123123"
              },
              "data": {
                "M": {
                  "formId": {
                    "S": "KM_HomeEdition_Short_155"
                  },
                  "firstName": {
                    "S": "John"
                  },
                  "lastName": {
                    "S": "Connor"
                  },
                  "emailAddress": {
                    "S": "example@email2.com"
                  },
                  "userType": {
                    "S": "user"
                  },
                  "passcode": {
                    "N": expect.any(String)
                  }
                }
              }
            }
          }
        }
      ]
    }
  }, expect.any(Function));

  expect(sendMessageBatchMock).toHaveBeenCalledWith({
    "Entries": [
      {
        "Id": "123",
        "MessageBody": "{\"id\":\"123\",\"lid\":\"123123\",\"data\":{\"formId\":\"KM_HomeEdition_Short_155\",\"firstName\":\"Jane\",\"lastName\":\"Connor\",\"emailAddress\":\"example@email.com\",\"userType\":\"admin\"}}"
      }
    ],
    "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/userQueue"
  }, expect.any(Function));

  awsMock.restore('DynamoDB.DocumentClient');
  awsMock.restore('SQS');
});

test('Should not trigger dynamodb when there is no admin user', async () => {
  process.env.TABLE_NAME = 'userTable';
  process.env.IDENTITY_LOOKUP_QUEUE = 'https://sqs.us-east-1.amazonaws.com/123456789012/userQueue';

  const eventExample = {
    records: {
      ['offset.1']: [{
        key: '1233456789',
        headers: [],
        offset: 1,
        partition: 1,
        timestamp: new Date().getTime(),
        topic: 'kafkaTopic',
        timestampType: 'CREATE_TIME',
        value: Buffer.from(JSON.stringify({
          eventType: 'FormDataProcessed',
          payload: {
            id: '123',
            lid: '123123',
            data: {
              formId: 'KM_HomeEdition_Short_155',
              firstName: 'Jane',
              lastName: 'Connor',
              emailAddress: 'example@email.com',
              userType: 'user'
            }
          }
        })).toString('base64')
      }],
      ['offset.2']: [{
        key: '1233456788',
        headers: [],
        offset: 2,
        partition: 1,
        timestamp: new Date().getTime(),
        topic: 'kafkaTopic',
        timestampType: 'CREATE_TIME',
        value: Buffer.from(JSON.stringify({
          eventType: 'FormDataProcessed',
          payload: {
            id: '123',
            lid: '123123',
            data: {
              formId: 'KM_HomeEdition_Short_155',
              firstName: 'John',
              lastName: 'Connor',
              emailAddress: 'example@email2.com',
              userType: 'user'
            }
          }
        })).toString('base64')
      }]
    },
    eventSource: 'aws:kafka',
    eventSourceArn: ''
  }

  const batchWriteMock = jest.fn().mockResolvedValue({
    promise: () => Promise.resolve(true)
  });
  const sendMessageBatchMock = jest.fn().mockResolvedValue({
    promise: () => Promise.resolve(true)
  });


  awsMock.setSDKInstance(aws);

  awsMock.mock('DynamoDB.DocumentClient', 'batchWrite', batchWriteMock);
  awsMock.mock('SQS', 'sendMessageBatch', sendMessageBatchMock);

  await handler(eventExample);
  expect(batchWriteMock).toHaveBeenCalledTimes(1);
  expect(sendMessageBatchMock).toHaveBeenCalledTimes(0);
  awsMock.restore('DynamoDB.DocumentClient');
  awsMock.restore('SQS');
});

test('Should not trigger sqs when there is no normal user', async () => {
  process.env.TABLE_NAME = 'userTable';
  process.env.IDENTITY_LOOKUP_QUEUE = 'https://sqs.us-east-1.amazonaws.com/123456789012/userQueue';

  const eventExample = {
    records: {
      ['offset.1']: [{
        key: '1233456789',
        headers: [],
        offset: 1,
        partition: 1,
        timestamp: new Date().getTime(),
        topic: 'kafkaTopic',
        timestampType: 'CREATE_TIME',
        value: Buffer.from(JSON.stringify({
          eventType: 'FormDataProcessed',
          payload: {
            id: '123',
            lid: '123123',
            data: {
              formId: 'KM_HomeEdition_Short_155',
              firstName: 'Jane',
              lastName: 'Connor',
              emailAddress: 'example@email.com',
              userType: 'admin'
            }
          }
        })).toString('base64')
      }],
      ['offset.2']: [{
        key: '1233456788',
        headers: [],
        offset: 2,
        partition: 1,
        timestamp: new Date().getTime(),
        topic: 'kafkaTopic',
        timestampType: 'CREATE_TIME',
        value: Buffer.from(JSON.stringify({
          eventType: 'FormDataProcessed',
          payload: {
            id: '123',
            lid: '123123',
            data: {
              formId: 'KM_HomeEdition_Short_155',
              firstName: 'John',
              lastName: 'Connor',
              emailAddress: 'example@email2.com',
              userType: 'admin'
            }
          }
        })).toString('base64')
      }]
    },
    eventSource: 'aws:kafka',
    eventSourceArn: ''
  }

  const batchWriteMock = jest.fn().mockResolvedValue({
    promise: () => Promise.resolve(true)
  });
  const sendMessageBatchMock = jest.fn().mockResolvedValue({
    promise: () => Promise.resolve(true)
  });

  awsMock.setSDKInstance(aws);

  awsMock.mock('DynamoDB.DocumentClient', 'batchWrite', batchWriteMock);
  awsMock.mock('SQS', 'sendMessageBatch', sendMessageBatchMock);

  await handler(eventExample);
  expect(batchWriteMock).toHaveBeenCalledTimes(0);
  expect(sendMessageBatchMock).toHaveBeenCalledTimes(1);
  awsMock.restore('DynamoDB.DocumentClient');
  awsMock.restore('SQS');

});
