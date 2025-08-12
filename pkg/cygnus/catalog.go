package cygnus

import (
	"context"
	fmt "fmt"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	identifierKey          = "identifier"
	namespaceKey           = "namespace"
	metadataLocationColumn = "p.metadata_location"
)

type Catalog struct {
	tableName string
	ddb       *dynamodb.Client
}

func NewCatalog(ddb *dynamodb.Client, tableName string) *Catalog {
	return &Catalog{
		tableName: tableName,
		ddb:       ddb,
	}
}

func (c *Catalog) LoadTable(ctx context.Context, identifier table.Identifier, _ iceberg.Properties) (*table.Table, error) {
	output, err := c.ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(c.tableName),
		Key: map[string]types.AttributeValue{
			identifierKey: &types.AttributeValueMemberS{Value: identifier[0]},
			namespaceKey:  &types.AttributeValueMemberS{Value: identifier[1]},
		},
	})
	if err != nil {
		return nil, err
	}

	metadataLocation, ok := output.Item[metadataLocationColumn].(*types.AttributeValueMemberS)
	if !ok {
		return nil, fmt.Errorf("metadata location not found")
	}

	return table.NewFromLocation(
		ctx,
		identifier,
		metadataLocation.Value,
		iceio.LoadFSFunc(nil, metadataLocation.Value),
		c,
	)
}

// It is a read-only catalog, so we don't need to implement this method.
func (c *Catalog) CommitTable(ctx context.Context, _ *table.Table, _ []table.Requirement, _ []table.Update) (table.Metadata, string, error) {
	panic("not implemented")
}
