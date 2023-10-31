package dynamolock_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"cirello.io/dynamolock"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type mockableDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI

	storage map[string]map[string]*dynamodb.AttributeValue

	getItemFunc    func(ctx context.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error)
	putItemFunc    func(ctx context.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error)
	updateItemFunc func(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error)
}

func (m *mockableDynamoDBClient) GetItemWithContext(ctx context.Context, input *dynamodb.GetItemInput, opts ...request.Option) (*dynamodb.GetItemOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if m.getItemFunc != nil {
		return m.getItemFunc(ctx, input, opts...)
	}

	item, ok := m.storage[*input.Key["key"].S]
	if !ok {
		return &dynamodb.GetItemOutput{}, nil
	}

	return &dynamodb.GetItemOutput{
		Item: item,
	}, nil
}

func (m *mockableDynamoDBClient) PutItemWithContext(ctx context.Context, input *dynamodb.PutItemInput, opts ...request.Option) (*dynamodb.PutItemOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if m.putItemFunc != nil {
		return m.putItemFunc(ctx, input, opts...)
	}

	fmt.Printf("Set RVN: '%s' (from '<none>')\n", *input.Item["recordVersionNumber"].S)
	m.storage[*input.Item["key"].S] = input.Item

	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockableDynamoDBClient) UpdateItemWithContext(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if m.updateItemFunc != nil {
		return m.updateItemFunc(ctx, input, opts...)
	}

	item, ok := m.storage[*input.Key["key"].S]
	if !ok {
		return nil, &dynamodb.ConditionalCheckFailedException{}
	}

	// RVN Assertion.
	if *item["recordVersionNumber"].S != *input.ExpressionAttributeValues[":0"].S {
		message := fmt.Sprintf("Expected RVN '%s', Received RVN '%s'", *item["recordVersionNumber"].S, *input.ExpressionAttributeValues[":0"].S)
		return nil, &dynamodb.ConditionalCheckFailedException{Message_: &message}
	}

	// Update RVN.
	fmt.Printf("Set RVN: '%s' (from '%s')\n", *input.ExpressionAttributeValues[":3"].S, *item["recordVersionNumber"].S)
	m.storage[*input.Key["key"].S]["recordVersionNumber"] = input.ExpressionAttributeValues[":3"]

	return &dynamodb.UpdateItemOutput{}, nil
}

// TestDynamoDBMisbehaves is meant to show the behavior of a client receiving an error that occured after DynamoDB successfully updates.
// This is reasonable to expect to happen since DynamoDB is not perfect and sometimes errors.
// Example local output:
/*
Set RVN: 'WdMebqJmzkQ3qdDK3CdRMXRzLmnNcPkH' (from '<none>')
Set RVN: 'jx8r17eJRsA3IyzeGj2nIkcoL0oa7gWr' (from 'WdMebqJmzkQ3qdDK3CdRMXRzLmnNcPkH')
Set RVN: 'qQoyCEWYE1O6GDkGEPAz8Oa4AvWFxK8v' (from 'jx8r17eJRsA3IyzeGj2nIkcoL0oa7gWr')
Set RVN: 'ORS1OicE1Nwb96OzMtExkx0Lty2o7zvu' (from 'qQoyCEWYE1O6GDkGEPAz8Oa4AvWFxK8v')
	sending heartbeat: already acquired lock, stopping heartbeats: ConditionalCheckFailedException: Expected RVN 'ORS1OicE1Nwb96OzMtExkx0Lty2o7zvu', Received RVN 'qQoyCEWYE1O6GDkGEPAz8Oa4AvWFxK8v'
*/
// This case should be recoverable, as we clearly *did* know the RVN we meant to set ('ORS1OicE1Nwb96OzMtExkx0Lty2o7zvu'), we just supplied the wrong one.
func TestDynamoDBMisbehaves(t *testing.T) {
	ctx := context.Background()

	ddb := &mockableDynamoDBClient{storage: make(map[string]map[string]*dynamodb.AttributeValue)}

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
	if err := c.SendHeartbeatWithContext(ctx, lockedItem); err != nil {
		err = fmt.Errorf("sending heartbeat: %w", err)
		t.Log(err)
		t.FailNow()
	}

	// Second heartbeat should work.
	if err := c.SendHeartbeatWithContext(ctx, lockedItem); err != nil {
		err = fmt.Errorf("sending heartbeat: %w", err)
		t.Log(err)
		t.FailNow()
	}

	// Return an error after this update, but succeed the update.
	ddb.updateItemFunc = func(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
		// Make future calls go to the default path.
		ddb.updateItemFunc = nil

		if _, err := ddb.UpdateItemWithContext(ctx, input, opts...); err != nil {
			return nil, fmt.Errorf("wrapped update should succeed: %w", err)
		}

		return nil, errors.New("transient error")
	}
	// Third heartbeat should fail.
	if err := c.SendHeartbeatWithContext(ctx, lockedItem); err == nil {
		err = errors.New("expected to error")
		t.Log(err)
		t.FailNow()
	}

	// Fourth heartbeat should work.
	if err := c.SendHeartbeatWithContext(ctx, lockedItem); err != nil {
		err = fmt.Errorf("sending heartbeat: %w", err)
		t.Log(err)
		t.FailNow()
	}
}

// TestDynamoDBHeartbeatTimesout is meant to show the behavior of a user setting a timeout when heartbeating and the timeout happening after the write but before we hear back.
// This is reasonable to expect to happen since it is often standard to enfore timeouts on all network calls since they can transiently hang for long periods of time.
/*
Set RVN: 'OiKg4SRIn9Ht0bg4HwXifdWD71VRgVGu' (from '<none>')
Set RVN: '3ZcgWghLHnBLCdnHHGVESYTHP7ryC5FF' (from 'OiKg4SRIn9Ht0bg4HwXifdWD71VRgVGu')
Set RVN: '3MGutn6tYADgi6q9x8oWjneceZDr5UpG' (from '3ZcgWghLHnBLCdnHHGVESYTHP7ryC5FF')
Set RVN: 'VzNk4hDuk4Kjb9zFuV8vMi2HQbOiKULB' (from '3MGutn6tYADgi6q9x8oWjneceZDr5UpG')
    sending heartbeat: already acquired lock, stopping heartbeats: ConditionalCheckFailedException: Expected RVN 'VzNk4hDuk4Kjb9zFuV8vMi2HQbOiKULB', Received RVN '3MGutn6tYADgi6q9x8oWjneceZDr5UpG'
*/
// This case should be recoverable, as we clearly *did* know the RVN we meant to set ('VzNk4hDuk4Kjb9zFuV8vMi2HQbOiKULB'), we just supplied the wrong one.
func TestDynamoDBHeartbeatTimesout(t *testing.T) {
	ctx := context.Background()

	ddb := &mockableDynamoDBClient{storage: make(map[string]map[string]*dynamodb.AttributeValue)}

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
	ddb.updateItemFunc = func(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...request.Option) (*dynamodb.UpdateItemOutput, error) {
		// Make future calls go to the default path.
		ddb.updateItemFunc = nil

		if _, err := ddb.UpdateItemWithContext(childCtx, input, opts...); err != nil {
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
