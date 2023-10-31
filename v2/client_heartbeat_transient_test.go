package dynamolock_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"cirello.io/dynamolock/v2"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type mockableDynamoDBClient struct {
	dynamolock.DynamoDBClient

	storage map[string]map[string]types.AttributeValue

	getItemFunc    func(ctx context.Context, input *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	putItemFunc    func(ctx context.Context, input *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	updateItemFunc func(ctx context.Context, input *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

func (m *mockableDynamoDBClient) GetItem(ctx context.Context, input *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if m.getItemFunc != nil {
		return m.getItemFunc(ctx, input, optFns...)
	}

	item, ok := m.storage[input.Key["key"].(*types.AttributeValueMemberS).Value]
	if !ok {
		return &dynamodb.GetItemOutput{}, nil
	}

	itemCopy := make(map[string]types.AttributeValue, len(item))
	for k, v := range item {
		itemCopy[k] = v
	}
	return &dynamodb.GetItemOutput{
		Item: itemCopy,
	}, nil
}

func (m *mockableDynamoDBClient) PutItem(ctx context.Context, input *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if m.putItemFunc != nil {
		return m.putItemFunc(ctx, input, optFns...)
	}

	fmt.Printf("Set RVN: '%s' (from '<none>')\n", input.Item["recordVersionNumber"].(*types.AttributeValueMemberS).Value)
	m.storage[input.Item["key"].(*types.AttributeValueMemberS).Value] = input.Item

	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockableDynamoDBClient) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if m.updateItemFunc != nil {
		return m.updateItemFunc(ctx, input, optFns...)
	}

	item, ok := m.storage[input.Key["key"].(*types.AttributeValueMemberS).Value]
	if !ok {
		return nil, &types.ConditionalCheckFailedException{}
	}

	// RVN Assertion.
	if item["recordVersionNumber"].(*types.AttributeValueMemberS).Value != input.ExpressionAttributeValues[":0"].(*types.AttributeValueMemberS).Value {
		message := fmt.Sprintf("Expected RVN '%s', Received RVN '%s'", item["recordVersionNumber"].(*types.AttributeValueMemberS).Value, input.ExpressionAttributeValues[":0"].(*types.AttributeValueMemberS).Value)
		return nil, &types.ConditionalCheckFailedException{Message: &message}
	}

	// Update RVN.
	fmt.Printf("Set RVN: '%s' (from '%s')\n", input.ExpressionAttributeValues[":3"].(*types.AttributeValueMemberS).Value, item["recordVersionNumber"].(*types.AttributeValueMemberS).Value)
	m.storage[input.Key["key"].(*types.AttributeValueMemberS).Value]["recordVersionNumber"] = input.ExpressionAttributeValues[":3"]

	return &dynamodb.UpdateItemOutput{}, nil
}

// TestDynamoDBMisbehaves is meant to show the behavior of a client receiving an error that occured after DynamoDB successfully updates.
// This is reasonable to expect to happen since DynamoDB is not perfect and sometimes errors.
// This recovers with retries on!
func TestDynamoDBMisbehaves(t *testing.T) {
	ctx := context.Background()

	ddb := &mockableDynamoDBClient{storage: make(map[string]map[string]types.AttributeValue)}

	c, err := dynamolock.New(ddb,
		"testtable",
		dynamolock.WithLeaseDuration(30*time.Second),
		dynamolock.WithHeartbeatPeriod(0),
	)
	if err != nil {
		err = fmt.Errorf("creating lock client: %w", err)
		t.Log(err)
		t.FailNow()
	}

	data := []byte("data")
	lockedItem, err := c.AcquireLock("lockName",
		dynamolock.WithData(data),
		dynamolock.ReplaceData(),
		dynamolock.WithAdditionalTimeToWaitForLock(time.Minute),
	)
	if err != nil {
		err = fmt.Errorf("acquiring lock: %w", err)
		t.Log(err)
		t.FailNow()
	}

	// Initial heartbeat should work.
	if err := c.SendHeartbeatWithContext(ctx, lockedItem, dynamolock.HeartbeatRetries(3, 0)); err != nil {
		err = fmt.Errorf("sending heartbeat: %w", err)
		t.Log(err)
		t.FailNow()
	}

	// Second heartbeat should work.
	if err := c.SendHeartbeatWithContext(ctx, lockedItem, dynamolock.HeartbeatRetries(3, 0)); err != nil {
		err = fmt.Errorf("sending heartbeat: %w", err)
		t.Log(err)
		t.FailNow()
	}

	// Return an error after this update, but succeed the update.
	ddb.updateItemFunc = func(ctx context.Context, input *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
		// Make future calls go to the default path.
		ddb.updateItemFunc = nil

		if _, err := ddb.UpdateItem(ctx, input, optFns...); err != nil {
			return nil, fmt.Errorf("wrapped update should succeed: %w", err)
		}

		return nil, errors.New("transient error")
	}
	// Third heartbeat should work thanks to retries.
	if err := c.SendHeartbeatWithContext(ctx, lockedItem, dynamolock.HeartbeatRetries(3, 0)); err != nil {
		err = errors.New("expected to error")
		t.Log(err)
		t.FailNow()
	}

	// Fourth heartbeat should work.
	if err := c.SendHeartbeatWithContext(ctx, lockedItem, dynamolock.HeartbeatRetries(3, 0)); err != nil {
		err = fmt.Errorf("sending heartbeat: %w", err)
		t.Log(err)
		t.FailNow()
	}
}

// TestDynamoDBHeartbeatTimesout is meant to show the behavior of a user setting a timeout when heartbeating and the timeout happening after the write but before we hear back.
// This is reasonable to expect to happen since it is often standard to enfore timeouts on all network calls since they can transiently hang for long periods of time.
/*
Set RVN: '1698715000021323000:VTGZIMOZDQ6LZ3AQ4N6HSXIOVZAF3WFW2XW3JIDYE35Y3DO2LBOQ' (from '<none>')
Set RVN: '1698715000021444000:OM2KIECNML3H6ZFWA7OPPRQVVB7KH3ZB5IEYOJGNS6NXDRLCF7GA' (from '1698715000021323000:VTGZIMOZDQ6LZ3AQ4N6HSXIOVZAF3WFW2XW3JIDYE35Y3DO2LBOQ')
Set RVN: '1698715000021491000:TJVM5TFX42KVB2ITLHJCFCC3JWBGUGXK3MD4BTCCCCYFHF34R7BQ' (from '1698715000021444000:OM2KIECNML3H6ZFWA7OPPRQVVB7KH3ZB5IEYOJGNS6NXDRLCF7GA')
Set RVN: '1698715000021518000:H6MMFRFUOU5J45TIM3QLXH23DHJ7KRX6YQPQ5KN2PVKXBZME4QZQ' (from '1698715000021491000:TJVM5TFX42KVB2ITLHJCFCC3JWBGUGXK3MD4BTCCCCYFHF34R7BQ')
    sending heartbeat: already acquired lock, stopping heartbeats: ConditionalCheckFailedException: Expected RVN '1698715000021518000:H6MMFRFUOU5J45TIM3QLXH23DHJ7KRX6YQPQ5KN2PVKXBZME4QZQ', Received RVN '1698715000021491000:TJVM5TFX42KVB2ITLHJCFCC3JWBGUGXK3MD4BTCCCCYFHF34R7BQ'
*/
// This case should be recoverable, as we clearly *did* know the RVN we meant to set ('1698715000021518000:H6MMFRFUOU5J45TIM3QLXH23DHJ7KRX6YQPQ5KN2PVKXBZME4QZQ'), we just supplied the wrong one.
func TestDynamoDBHeartbeatTimesout(t *testing.T) {
	ctx := context.Background()

	ddb := &mockableDynamoDBClient{storage: make(map[string]map[string]types.AttributeValue)}

	c, err := dynamolock.New(ddb,
		"testtable",
		dynamolock.WithLeaseDuration(30*time.Second),
		dynamolock.WithHeartbeatPeriod(0),
	)
	if err != nil {
		err = fmt.Errorf("creating lock client: %w", err)
		t.Log(err)
		t.FailNow()
	}

	data := []byte("data")
	lockedItem, err := c.AcquireLock("lockName",
		dynamolock.WithData(data),
		dynamolock.ReplaceData(),
		dynamolock.WithAdditionalTimeToWaitForLock(time.Minute),
	)
	if err != nil {
		err = fmt.Errorf("acquiring lock: %w", err)
		t.Log(err)
		t.FailNow()
	}

	// Initial heartbeat should work.
	childCtx, cancel := context.WithCancel(ctx)
	if err := c.SendHeartbeatWithContext(childCtx, lockedItem); err != nil {
		err = fmt.Errorf("sending heartbeat: %w", err)
		t.Log(err)
		t.FailNow()
	}
	cancel()

	// Second heartbeat should work.
	childCtx, cancel = context.WithCancel(ctx)
	if err := c.SendHeartbeatWithContext(ctx, lockedItem); err != nil {
		err = fmt.Errorf("sending heartbeat: %w", err)
		t.Log(err)
		t.FailNow()
	}
	cancel()

	// Return an error after this update, but succeed the update.
	childCtx, cancel = context.WithCancel(ctx)
	ddb.updateItemFunc = func(ctx context.Context, input *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
		// Make future calls go to the default path.
		ddb.updateItemFunc = nil

		if _, err := ddb.UpdateItem(childCtx, input, optFns...); err != nil {
			return nil, fmt.Errorf("wrapped update should succeed: %w", err)
		}

		cancel()
		return nil, childCtx.Err()
	}
	// Third heartbeat should fail.
	if err := c.SendHeartbeatWithContext(ctx, lockedItem); err == nil {
		err = errors.New("expected to error")
		t.Log(err)
		t.FailNow()
	}

	// Fourth heartbeat should work.
	childCtx, cancel = context.WithCancel(ctx)
	if err := c.SendHeartbeatWithContext(ctx, lockedItem); err != nil {
		err = fmt.Errorf("sending heartbeat: %w", err)
		t.Log(err)
		t.FailNow()
	}
	cancel()
}
