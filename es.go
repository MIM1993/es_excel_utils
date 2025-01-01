/*
@Time : 2024/12/18 14:54
@Author : SunJianChao
@File : es.go
@Software : GoLand
*/

package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/olivere/elastic/v7"
	uuid "github.com/satori/go.uuid"
	"log"
	"time"
)

var (
	EsClient   *elastic.Client
	esUrl      = ""
	esUserName = ""
	esPassWord = ""
)

func initEs() *elastic.Client {
	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(esUrl),
		elastic.SetBasicAuth(esUserName, esPassWord))
	if err != nil {
		panic("connect es failed!")
	}
	return client
}

func CreateIndex(esIndex, mapping string) (*elastic.IndicesCreateResult, error) {
	ctx := context.Background()
	exists, err := EsClient.IndexExists(esIndex).Do(ctx)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, errors.New("index is exists")
	}
	res, err := EsClient.CreateIndex(esIndex).BodyString(mapping).Do(ctx)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func DeleteIndex(esIndex string) (*elastic.IndicesDeleteResponse, error) {
	ctx := context.Background()
	exists, err := EsClient.IndexExists(esIndex).Do(ctx)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("index is not exists")
	}
	res, err := EsClient.DeleteIndex(esIndex).Do(ctx)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func Insert(esIndex, id string, esBody interface{}) (*elastic.IndexResponse, error) {
	res, err := EsClient.Index().
		Index(esIndex).
		BodyJson(esBody).
		Id(id).Refresh("true").
		Do(context.Background())
	if err != nil {
		return nil, err
	}
	return res, nil
}

func BatchInsert(esIndex string, esBody []interface{}) error {
	ctx := context.Background()
	exists, err := EsClient.IndexExists(esIndex).Do(ctx)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("esIndex not exists")
	}
	w, err := EsClient.BulkProcessor().BulkActions(500).
		FlushInterval(time.Millisecond).Workers(20).Stats(true).
		After(GetFailed).Do(context.Background())
	if err != nil {
		return err
	}
	err = w.Start(ctx)
	if err != nil {
		return err
	}
	defer w.Close()
	for _, data := range esBody {
		req := elastic.NewBulkIndexRequest().Index(esIndex).Id(uuid.NewV4().String()).Doc(data)
		w.Add(req)
	}
	return nil
}

func GetFailed(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if response == nil {
		log.Println("GetNil response return")
		return
	}
	fi := response.Failed()
	if len(fi) != 0 {
		for _, f := range fi {
			log.Printf("DebugFailedEs: index:%s type:%s id:%s version:%d  status:%d result:%s ForceRefresh:%v errorDetail:%v getResult:%v\n", f.Index, f.Type, f.Id, f.Version, f.Status, f.Result, f.ForcedRefresh, f.Error, f.GetResult)
		}
	}
}

func Search(esIndex string, mapping map[string]interface{}, highlight *elastic.Highlight) (*elastic.SearchResult, error) {

	queryJSON, err := json.Marshal(mapping)
	if err != nil {
		return nil, err
	}
	searchResult, err := EsClient.Search().
		Index(esIndex).
		Source(string(queryJSON)).
		Highlight(highlight).
		Do(context.Background())
	if err != nil {
		return nil, err
	}
	return searchResult, nil
}

func Delete(esIndex, id string) (*elastic.DeleteResponse, error) {
	res, err := EsClient.Delete().
		Index(esIndex).
		Id(id).Refresh("true").
		Do(context.Background())
	if err != nil {
		return nil, err
	}
	return res, nil
}

func Update(esIndex, id string, esBody interface{}) (*elastic.UpdateResponse, error) {
	res, err := EsClient.
		Update().
		Index(esIndex).
		Id(id).
		Doc(esBody).
		Refresh("true").
		Do(context.Background())
	if err != nil {
		return nil, err
	}
	return res, err
}
