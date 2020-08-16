package cosmosdb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rancher/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Etcd Object
type Etcd struct {
	ID          	primitive.ObjectID 	`bson:"_id,omitempty"`
	Key            	string 				`bson:"key"`
	CreateRevision 	primitive.ObjectID  `bson:"createrevision"`
	ModRevision    	primitive.ObjectID  `bson:"modrevision"`
	Version         int64				`bson:"version"`
	Value          	[]byte  			`bson:"value"`
	Lease          	int64   			`bson:"lease"`
}

type CosmosDb struct {
	connStr       string
	database      string
	collectionStr string

	client *mongo.Client
	collection *mongo.Collection
}

func (db *CosmosDb) Start(ctx context.Context) error {
	return nil
}

func (db *CosmosDb) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	return 0, nil, false, errors.New("Delete broke bb")
}

type countStruct struct {
	count int64
}

func (db *CosmosDb) Count(ctx context.Context, prefix string) (int64, int64, error) {
	logrus.Debugf("COUNT %s", prefix)

	if err := db.connect(); err != nil {
		return 0, 0, err
	}

	regex := prefix
	if strings.HasSuffix(prefix, "/") {
		regex += ".*"
	}

	filter := primitive.D{{"$match", primitive.D{{"$and", primitive.A{primitive.D{{"key", primitive.D{{"$regex", regex}}}}}}}}}
	count := primitive.D{{"$count", "count"}}


	res, err := db.collection.Aggregate(ctx, mongo.Pipeline{filter, count})
	if err != nil {
		logrus.Error(fmt.Sprintf("Count after aggregate %v", err))
		return 0, 0, err
	}

	var list []countStruct
	if err = res.All(ctx, &list); err != nil {
		logrus.Error(fmt.Sprintf("Count after iteration %s", err))
		return 0, 0, err
	}

	rev, err := db.getGlobalRevision(ctx)
	if err != nil {
		return 0, 0, err
	}
	return rev, list[0].count, nil
}

func New(ctx context.Context, dataSourceName string) (*CosmosDb, error) {
	logrus.Print("Creating CosmosDb Driver")
	driver := &CosmosDb{}

	if err := driver.prepareCosmosDb(dataSourceName); err != nil {
		return nil, err
	}
	if err := driver.connect(); err != nil {
		return nil, err
	}
	modIndex := mongo.IndexModel{
		Keys: primitive.M{
			"modrevision": 1,
		},
		Options: nil,
	}
	_, err := driver.collection.Indexes().CreateOne(ctx, modIndex)
	if err != nil {
		return nil, err
	}

	return driver, nil

}

func (db *CosmosDb) getGlobalRevision(ctx context.Context) (int64, error) {
	if err := db.connect(); err != nil {
		return 0, err
	}

	var filter primitive.D
	ops := options.Find()
	ops.SetSort(primitive.D{{"_id", -1}})
	ops.SetLimit(1)

	rs, err := db.collection.Find(ctx, filter, ops)
	if err != nil {
		return 0, err
	}

	var revision []Etcd
	err = rs.All(ctx, &revision)
	if err != nil {
		logrus.Errorf("getGlobalRevision: %s", err)
		return 0, err
	}
	return revision[0].ID.Timestamp().UnixNano(), nil
}

func (db *CosmosDb) prepareCosmosDb(dataSourceName string) error {
	db.connStr = "mongodb://" + dataSourceName
	db.database = "kine"
	db.collectionStr = "etcd"

	return nil
}

// connects to MongoDB
func (db *CosmosDb) connect() error {
	ctx, cancel:= context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()
	logrus.Debug("attempting to connect to mongodb")

	clientOptions := options.Client().ApplyURI(db.connStr).SetDirect(true)
	c, err := mongo.NewClient(clientOptions)
	if err != nil {
		logrus.Error(fmt.Sprintf("connect error: %v", err))
		return err
	}
	err = c.Connect(ctx)
	if err != nil {
		return err
	}

	err = c.Ping(ctx, nil)
	if err != nil {
		return err
	}

	db.client =  c

	db.collection = db.client.Database(db.database).Collection(db.collectionStr)

	return nil
}

func (db *CosmosDb) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	logrus.Debugf("CREATE %s", key)

	if err := db.connect(); err != nil {
		return 0, err
	}

	kineCollection := db.client.Database(db.database).Collection(db.collectionStr)

	objectId := primitive.NewObjectIDFromTimestamp(time.Now())

	_, err := kineCollection.InsertOne(context.TODO(),
		Etcd{
			Key: key,
			Value: value,
			Lease: lease,
			CreateRevision: objectId,
			Version: 1,
			ModRevision: objectId,
		},
	)
	if err != nil {
		logrus.Error(fmt.Sprintf("Create: %v", err))
		return 0, err
	}

	return objectId.Timestamp().UnixNano(), nil
}

func (db *CosmosDb) Get(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, error) {
	logrus.Debugf("GET %s, revision=%d", key, revision)

	if err := db.connect(); err != nil {
		return 0, nil, err
	}

	kineCollection := db.client.Database(db.database).Collection(db.collectionStr)

	filter := primitive.D{
		{"key", key},
		{"modrevision", primitive.D{{"$lte", primitive.NewObjectIDFromTimestamp(time.Unix(0, revision))}}}}

	ops := options.FindOne()
	ops.SetSort(primitive.D{{"modrevision", -1}})

	result := kineCollection.FindOne(context.TODO(), filter, ops)
	var out Etcd
	if err := result.Decode(&out); err != nil {
		logrus.Error("GET: %v", err)
		if err.Error() == "mongo: no documents in result" {
			currRev, err := db.getGlobalRevision(context.TODO())
			if err != nil {
				return 0, nil, err
			}
			return currRev, nil, nil
		}
		return 0, nil, err
	}

	currRev, err := db.getGlobalRevision(context.TODO())
	if err != nil {
		return 0, nil, err
	}

	kv := &server.KeyValue{
		Key:            out.Key,
		CreateRevision: out.CreateRevision.Timestamp().UnixNano(),
		ModRevision:    out.ModRevision.Timestamp().UnixNano(),
		Value:          out.Value,
		Lease:          out.Lease,
	}
	return currRev, kv, nil
}

func (db *CosmosDb) List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, []*server.KeyValue, error) {
	logrus.Debugf("LIST %s, revision=%d", prefix, revision)

	if err := db.connect(); err != nil {
		return 0, nil, err
	}

	var rev primitive.ObjectID

	if revision == 0 {
		rev = primitive.NewObjectIDFromTimestamp(time.Now())
	} else {
		rev = primitive.NewObjectIDFromTimestamp(time.Unix(0, revision))
	}

	regex := prefix + ".*"

	filter := primitive.D{{"$match", primitive.D{{"$and", primitive.A{primitive.D{{"key", primitive.D{{"$regex", regex}}}}, primitive.D{{"modrevision", primitive.D{{"$lte", rev}}}}}}}}}
	group := primitive.D{{"$group", primitive.D{
		{"_id", "$key"},
		{"max_rev", primitive.D{{"$max", "$modrevision"}}},
		{"records", primitive.D{{"$push", "$$ROOT"}}}}}}

	project := primitive.D{{"$project", primitive.D{{"record", primitive.D{{"$filter", primitive.D{{"input", "$records"}, {"as", "doc"}, {"cond",
		primitive.D{{"$and", primitive.A{primitive.D{{"$eq", primitive.A{"$max_rev", "$$doc.modrevision"}}}}}}}}}}}}}}

	unwind := primitive.D{{"$unwind", "$record"}}

	replace := primitive.D{{"$replaceRoot", primitive.D{{"newRoot", "$record"}}}}

	limitStage := primitive.D{{"$limit", limit}}

	var pipeline mongo.Pipeline

	if limit > 0 {
		pipeline = mongo.Pipeline{filter, group, project, limitStage, unwind, replace}
	} else {
		pipeline = mongo.Pipeline{filter, group, project, unwind, replace}
	}

	logrus.Debugf("LIST pipeline: %s", pipeline)
	res, err := db.collection.Aggregate(context.TODO(), pipeline)
	if err != nil {
		logrus.Errorf("List after agggregate: %v", err)
		return 0, nil, err
	}

	var list []Etcd
	if err = res.All(context.TODO(), &list); err != nil {
		logrus.Errorf("List after iterate: %v", err)
		return 0, nil, err
	}

	var outList []*server.KeyValue
	for _, val := range list {
			outList = append(outList, &server.KeyValue{
				Key:            val.Key,
				CreateRevision: val.CreateRevision.Timestamp().UnixNano(),
				ModRevision:    val.ModRevision.Timestamp().UnixNano(),
				Value:          val.Value,
				Lease:          val.Lease,
			})
	}
	currRev, err := db.getGlobalRevision(ctx)
	if err != nil {
		return 0, nil, err
	}
	return currRev, outList, nil
}

func (db *CosmosDb) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	logrus.Debugf("UPDATE %s, revision=%d", key, revision)

	_, event, err := db.Get(ctx, key, time.Now().UnixNano())
	if err != nil {
		return 0, nil, false, err
	}

	if event == nil {
		return 0, nil, false, nil
	}

	if err := db.connect(); err != nil {
		return 0, nil, false, err
	}

	kineCollection := db.client.Database(db.database).Collection(db.collectionStr)

	objectId := primitive.NewObjectIDFromTimestamp(time.Now())

	update := &Etcd{
		Key: key,
		Value: value,
		Lease: lease,
		CreateRevision: primitive.NewObjectIDFromTimestamp(time.Unix(0, event.CreateRevision)),
		ModRevision: objectId,
	}
	_, err = kineCollection.InsertOne(context.TODO(),
		*update,
	)
	if err != nil {
		logrus.Error(fmt.Sprintf("Update after insert: %v", err))
		return 0, nil, false, nil
	}
	out := &server.KeyValue{
		Key:            key,
		CreateRevision: event.CreateRevision,
		ModRevision:    objectId.Timestamp().UnixNano(),
		Value:          value,
		Lease:          lease,
	}
	return objectId.Timestamp().UnixNano(), out, true, nil
}

func (db *CosmosDb) Watch(ctx context.Context, prefix string, revision int64) <-chan []*server.Event {
	if err := db.connect(); err != nil {
		return nil
	}

	logrus.Debugf("WATCH %s, revision=%d", prefix, revision)

	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	//TODO: Actually make this work and use change streams
	result := make(chan []*server.Event, 100)

	rev, kvs, err := db.List(ctx, prefix, "", revision, 0)
	if err != nil {
		logrus.Errorf("failed to list %s for revision %d", prefix, revision)
		cancel()
	}

	logrus.Debugf("WATCH LIST key=%s rev=%d => rev=%d kvs=%d", prefix, revision, rev, len(kvs))

	return result
}

func (db *CosmosDb) startWatch(ctx context.Context, stream *mongo.ChangeStream, channel chan []*server.Event) {
	return
}
