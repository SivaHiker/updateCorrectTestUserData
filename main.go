package main

import (
	"gopkg.in/mgo.v2"
	"fmt"
	"gopkg.in/mgo.v2/bson"
)

var jobs chan UserInfo
var done chan bool
var counter int64
var activeCounter int64
var inactiveCounter int64
var c1 *mgo.Collection
var c *mgo.Collection


func main(){
	jobs = make(chan UserInfo, 10000)
	//done = make(chan bool, 1)


	session1, err1 := mgo.Dial("10.15.0.145")
	if err1 != nil {
		panic(err1)
	}
	defer session1.Close()

	c1 = session1.DB("userlist").C("newuserdata")




	session, err := mgo.Dial("10.15.0.77")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c = session.DB("userdb").C("users")

	var result UserInfo
	for w := 1; w <= 500; w++ {
		go workerPool()
	}

	find := c1.Find(bson.M{})
	items := find.Iter()
	for items.Next(&result) {
		jobs <- result
	}
	<-done
}


func workerPool() {
	for (true) {
		select {
		case result, ok := <-jobs:
			if ok {
				resuser := UserRecord{}
				userMissdn := result.UserData.Msisdn
				query := bson.M{"msisdn": userMissdn}
				res := c.Find(query).One(&resuser)
				if (resuser.Status == 1 || resuser.Status == 2) {
					err := c1.Update(res, bson.M{"$set": bson.M{"active": false}})
					fmt.Println(err)
					counter++
				}
			fmt.Println("Total Active User records  --- >", counter)
		  }
		case <-done:
			done<-true
		}
	}

}

type UserRecord struct {
	ID       bson.ObjectId 		`bson:"_id,omitempty"`
	Addressbook   struct{}      `json:"addressbook"`
	BackupToken   string        `json:"backup_token"`
	Connect       int           `json:"connect"`
	Country       string        `json:"country"`
	Devices       []DeviceDetails `json:"devices"`
	Gender        string        `json:"gender"`
	Icon          string        `json:"icon"`
	InvitedJoined []interface{} `json:"invited_joined"`
	Invitetoken   string        `json:"invitetoken"`
	Locale        string        `json:"locale"`
	Msisdn        []string      `json:"msisdn"`
	Name          string        `json:"name"`
	PaUID         string        `json:"pa_uid"`
	Referredby    []string      `json:"referredby"`
	RewardToken   string        `json:"reward_token"`
	Status        int           `json:"status"`
	Sus           int           `json:"sus"`
	Uls           int           `json:"uls"`
	Version       int           `json:"version"`
}

type DeviceDetails struct {
	Sound            string      `json:"sound"`
	DevID            string      `json:"dev_id"`
	RegTime          int         `json:"reg_time"`
	Preview          bool        `json:"preview"`
	Staging          bool        `json:"staging"`
	Os               string      `json:"os"`
	DevToken         string      `json:"dev_token"`
	DevVersion       string      `json:"dev_version"`
	Msisdn           string      `json:"msisdn"`
	UpgradeTime      int         `json:"upgrade_time"`
	Pdm              string      `json:"pdm"`
	DevType          string      `json:"dev_type"`
	Token            string      `json:"token"`
	OsVersion        string      `json:"os_version"`
	Resolution       string      `json:"resolution"`
	DeviceKey        interface{} `json:"device_key"`
	LastActivityTime int         `json:"last_activity_time"`
	AppVersion       string      `json:"app_version"`
	DevTokenUpdateTs int         `json:"dev_token_update_ts"`
}

type UserInfo struct {
	ID       bson.ObjectId `bson:"_id,omitempty"`
	UserData UserData `json:"UserData"`
	Flag   bool `json:"flag"`
	Active bool `json:"active"`
}

//type UserData struct {
//	Msisdn string `json:"msisdn"`
//	Token  string `json:"token"`
//	UID    string `json:"uid"`
//}

type UserData struct {
	Msisdn        string `json:"msisdn"`
	Token         string `json:"token"`
	UID           string `json:"uid"`
	Platformuid   string `json:"platformuid"`
	Platformtoken string `json:"platformtoken"`
}

