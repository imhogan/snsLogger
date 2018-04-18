'use strict';

console.log('Loading function');

exports.handler = (event, context, callback) => {

    console.log('Version 1.0.4');

    // Simple logger function which takes account of the logLevel in the invokation event.
    function myLog(level, ...message) {
        
        if (process.env.LOG_LEVEL === null || level < process.env.LOG_LEVEL) {
            console.log(message);
        }
    }

    myLog(2, 'Received event:', JSON.stringify(event, null, 2));
    
    var result = "";

    event.Records.forEach((record) => {
        const snsEvent = record.Sns;
    
        const message = snsEvent.Message;
        
        myLog(2, 'From SNS:', message);
        
        try {
            
            var AWS = require("aws-sdk");
            
            var docClient = new AWS.DynamoDB.DocumentClient();
            
            var params = {
                TableName: process.env.LOG_TABLE,
                Item:{
                    "TopicArn": snsEvent.TopicArn,
                    "Timestamp": snsEvent.Timestamp,
                    "Event": snsEvent
                }
            };
        
            myLog(4,"Adding a new item...");
            docClient.put(params, function(err, data) {
                if (err) {
                    myLog(0,"Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
                } else {
                    myLog(4,"Added item:", JSON.stringify(data, null, 2));
                }
            });
            
            if (message.startsWith('{')) {
                var sesEvent = JSON.parse(message);
                
                var EventType = sesEvent.eventType;
                
                if (EventType === 'Click' && sesEvent.click && sesEvent.click.linkTags && sesEvent.click.linkTags.id) {
                    EventType += '-'+sesEvent.click.linkTags.id[0];
                }
                myLog(4,'EventType is ', EventType);
                
                sesEvent.mail.destination.forEach((recipient) => {
                    myLog(4,'Recipient is ', recipient);
                    
                    var updateParams = {
                        TableName:process.env.EMAIL_TABLE,
                        Key:{
                            "Recipient": recipient,
                            "MessageID": sesEvent.mail.commonHeaders.messageId
                        },
                        UpdateExpression: "SET subject = :sub, sender = :from ADD #event :e",
                        ExpressionAttributeNames:{
                            "#event": EventType
                        },
                        ExpressionAttributeValues:{
                            ":sub":sesEvent.mail.commonHeaders.subject,
                            ":from":sesEvent.mail.commonHeaders.from,
                            ":e":docClient.createSet([sesEvent.mail.timestamp])
                        },
                        ReturnValues:"UPDATED_NEW"
                    };
                    
                    docClient.update(updateParams, function(err, data) {
                        if (err) {
                            myLog(0,"Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
                            throw {
                                 name:'Update Failed'
                                ,description:"Unable to update. Error:"+JSON.stringify(err, null, 2)
                            };
                        } else {
                            myLog(4,"Added item:", JSON.stringify(data, null, 2));
                        }
                    });
                });
            }

            result += "processed MessageId "+snsEvent.MessageId+"\n";
            
        }
        catch (ex) {
            myLog(0, "Exception:", JSON.stringify(ex, null, 2));
            callback(ex,"Exception:", JSON.stringify(ex, null, 2));
        }
    });
    
    callback(null, result);
};
