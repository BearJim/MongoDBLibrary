package MongoDBLibrary

import (
	"context"
	"encoding/json"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Client struct {
	mongoClient *mongo.Client
	db          *mongo.Database
}

func New(dbName string, url string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	defer cancel()
	if err != nil {
		return nil, err
	}

	c := &Client{
		mongoClient: client,
		db:          client.Database(dbName),
	}
	return c, nil
}

func (c *Client) RestfulAPIGetOne(collName string, filter bson.M) map[string]interface{} {
	collection := c.db.Collection(collName)

	var result map[string]interface{}
	collection.FindOne(context.TODO(), filter).Decode(&result)

	return result
}

func (c *Client) RestfulAPIGetMany(collName string, filter bson.M) ([]map[string]interface{}, error) {
	collection := c.db.Collection(collName)

	var resultArray []map[string]interface{}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	cur, err := collection.Find(ctx, filter)
	defer cancel()
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var result map[string]interface{}
		err := cur.Decode(&result)
		if err != nil {
			return nil, err
		}
		resultArray = append(resultArray, result)
	}
	if err := cur.Err(); err != nil {
		return nil, err

	}
	return resultArray, nil
}

func (c *Client) RestfulAPIPutOne(collName string, filter bson.M, putData map[string]interface{}) bool {
	collection := c.db.Collection(collName)

	var checkItem map[string]interface{}
	collection.FindOne(context.TODO(), filter).Decode(&checkItem)

	if checkItem == nil {
		collection.InsertOne(context.TODO(), putData)
		return false
	} else {
		collection.UpdateOne(context.TODO(), filter, bson.M{"$set": putData})
		return true
	}
}

func (c *Client) RestfulAPIPutOneNotUpdate(collName string, filter bson.M, putData map[string]interface{}) bool {
	collection := c.db.Collection(collName)

	var checkItem map[string]interface{}
	collection.FindOne(context.TODO(), filter).Decode(&checkItem)

	if checkItem == nil {
		collection.InsertOne(context.TODO(), putData)
		return false
	} else {
		// collection.UpdateOne(context.TODO(), filter, bson.M{"$set": putData})
		return true
	}
}

func (c *Client) RestfulAPIPutMany(collName string, filterArray []bson.M, putDataArray []map[string]interface{}) bool {
	collection := c.db.Collection(collName)

	var checkItem map[string]interface{}
	for i, putData := range putDataArray {
		checkItem = nil
		filter := filterArray[i]
		collection.FindOne(context.TODO(), filter).Decode(&checkItem)

		if checkItem == nil {
			collection.InsertOne(context.TODO(), putData)
		} else {
			collection.UpdateOne(context.TODO(), filter, bson.M{"$set": putData})
		}
	}

	if checkItem == nil {
		return false
	} else {
		return true
	}

}

func (c *Client) RestfulAPIDeleteOne(collName string, filter bson.M) {
	collection := c.db.Collection(collName)

	collection.DeleteOne(context.TODO(), filter)
}

func (c *Client) RestfulAPIDeleteMany(collName string, filter bson.M) {
	collection := c.db.Collection(collName)

	collection.DeleteMany(context.TODO(), filter)
}

func (c *Client) RestfulAPIMergePatch(collName string, filter bson.M, patchData map[string]interface{}) (bool, error) {
	collection := c.db.Collection(collName)

	var originalData map[string]interface{}
	result := collection.FindOne(context.TODO(), filter)

	if err := result.Decode(&originalData); err != nil { // Data doesn't exist in DB
		return false, err
	} else {
		delete(originalData, "_id")
		original, _ := json.Marshal(originalData)

		patchDataByte, err := json.Marshal(patchData)
		if err != nil {
			return false, err
		}

		modifiedAlternative, err := jsonpatch.MergePatch(original, patchDataByte)
		if err != nil {
			return false, err
		}

		var modifiedData map[string]interface{}

		json.Unmarshal(modifiedAlternative, &modifiedData)
		collection.UpdateOne(context.TODO(), filter, bson.M{"$set": modifiedData})
		return true, nil
	}
}

func (c *Client) RestfulAPIJSONPatch(collName string, filter bson.M, patchJSON []byte) (bool, error) {
	collection := c.db.Collection(collName)

	var originalData map[string]interface{}
	result := collection.FindOne(context.TODO(), filter)

	if err := result.Decode(&originalData); err != nil { // Data doesn't exist in DB
		return false, err
	} else {
		delete(originalData, "_id")
		original, _ := json.Marshal(originalData)

		patch, err := jsonpatch.DecodePatch(patchJSON)
		if err != nil {
			return false, err
		}

		modified, err := patch.Apply(original)
		if err != nil {
			return false, err
		}

		var modifiedData map[string]interface{}

		json.Unmarshal(modified, &modifiedData)
		collection.UpdateOne(context.TODO(), filter, bson.M{"$set": modifiedData})
		return true, nil
	}

}

func (c *Client) RestfulAPIJSONPatchExtend(collName string, filter bson.M, patchJSON []byte, dataName string) (bool, error) {
	collection := c.db.Collection(collName)

	var originalDataCover map[string]interface{}
	result := collection.FindOne(context.TODO(), filter)

	if err := result.Decode(&originalDataCover); err != nil { // Data does'nt exist in db
		return false, err
	} else {
		delete(originalDataCover, "_id")
		originalData := originalDataCover[dataName]
		original, _ := json.Marshal(originalData)

		jsonpatch.DecodePatch(patchJSON)
		patch, err := jsonpatch.DecodePatch(patchJSON)
		if err != nil {
			return false, err
		}

		modified, err := patch.Apply(original)
		if err != nil {
			return false, err
		}

		var modifiedData map[string]interface{}
		json.Unmarshal(modified, &modifiedData)
		collection.UpdateOne(context.TODO(), filter, bson.M{"$set": bson.M{dataName: modifiedData}})
		return true, nil
	}
}

func (c *Client) InsertOne(collName string, data interface{}) (*mongo.InsertOneResult, error) {
	collection := c.db.Collection(collName)
	return collection.InsertOne(context.TODO(), data)
}

func (c *Client) Database() *mongo.Database {
	return c.db
}

func (c *Client) FindOne(collName string, filter interface{}) *mongo.SingleResult {
	collection := c.db.Collection(collName)
	return collection.FindOne(context.TODO(), filter)
}

func (c *Client) RestfulAPIPost(collName string, filter bson.M, postData map[string]interface{}) bool {
	collection := c.db.Collection(collName)

	var checkItem map[string]interface{}
	collection.FindOne(context.TODO(), filter).Decode(&checkItem)

	if checkItem == nil {
		collection.InsertOne(context.TODO(), postData)
		return false
	} else {
		collection.UpdateOne(context.TODO(), filter, bson.M{"$set": postData})
		return true
	}
}

func (c *Client) RestfulAPIPostMany(collName string, filter bson.M, postDataArray []interface{}) bool {
	collection := c.db.Collection(collName)

	collection.InsertMany(context.TODO(), postDataArray)
	return false
}

func (c *Client) Watch(ctx context.Context, collName string, pipeline mongo.Pipeline) (*mongo.ChangeStream, error) {
	collection := c.db.Collection(collName)
	return collection.Watch(ctx, pipeline, options.ChangeStream().SetFullDocument(options.UpdateLookup))
}
