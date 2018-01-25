package main

import (
	"gopkg.in/mgo.v2"
	"fmt"
	"gopkg.in/mgo.v2/bson"
)

var jobs chan NewUserRecord
var done chan bool
var counter int64
var activeCounter int64
var inactiveCounter int64
var c1 *mgo.Collection
var c *mgo.Collection


func main(){
	jobs = make(chan NewUserRecord, 10000)
	//done = make(chan bool, 1)


	session1, err1 := mgo.Dial("10.15.0.145")
	if err1 != nil {
		panic(err1)
	}
	defer session1.Close()

	c1 = session1.DB("userlist").C("activeuserdata")




	session, err := mgo.Dial("10.15.0.75")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c = session.DB("userdb").C("users")

	var result NewUserRecord
	for w := 1; w <= 500; w++ {
		go workerPool()
	}

	find := c.Find(bson.M{})
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
				if (result.Status == 1 || result.Status == 2) {
					continue
				} else {
					var userdata UserInfo
					if(len(result.Msisdn) >0 && len(result.Devices) >0) {
						userdata.UserData.Msisdn = result.Msisdn[0]
						uid := result.Md.UID
						token := result.Devices[0].Token
						if (uid != "" && token != "") {
							userdata.UserData.UID = uid
							userdata.UserData.Token = token
							userdata.Flag = false
							userdata.Active = true
							err := c1.Insert(userdata)
							fmt.Println(err)
							counter++
						}
					}
				}
			if(counter == 30000000){
				done<-true
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


type NewUserRecord struct {
	ID      string   `json:"_id"`
	Msisdn  []string `json:"msisdn"`
	Country string   `json:"country"`
	Devices []struct {
		DevID            string `json:"dev_id"`
		RegTime          int    `json:"reg_time"`
		DevTokenUpdateTs int    `json:"dev_token_update_ts"`
		DevToken         string `json:"dev_token"`
		DevVersion       string `json:"dev_version"`
		Msisdn           string `json:"msisdn"`
		Resolution       string `json:"resolution"`
		Token            string `json:"token"`
		PhL              string `json:"ph_l"`
		DevType          string `json:"dev_type"`
		Pdm              string `json:"pdm"`
		OsVersion        string `json:"os_version"`
		DeviceKey        string `json:"device_key"`
		LastActivityTime int    `json:"last_activity_time"`
		AppVersion       string `json:"app_version"`
		Os               string `json:"os"`
		ApL              string `json:"ap_l"`
	} `json:"devices"`
	Rewards4 struct {
		Tt    int `json:"tt"`
		Total int `json:"total"`
	} `json:"rewards4"`
	RewardToken string `json:"reward_token"`
	BackupToken string `json:"backup_token"`
	Gender      string `json:"gender"`
	Name        string `json:"name"`
	Invitetoken string `json:"invitetoken"`
	Md          struct {
		UID string `json:"uid"`
	} `json:"md"`
	Locale      string `json:"locale"`
	PaUID       string `json:"pa_uid"`
	Addressbook struct {
	} `json:"addressbook"`
	InvitedJoined []interface{} `json:"invited_joined"`
	Sus           int           `json:"sus"`
	Setting       string        `json:"setting"`
	Uls           int           `json:"uls"`
	Lastseen      bool          `json:"lastseen"`
	Avatar        int           `json:"avatar"`
	Status        int           `json:"status"`
	Dob           struct {
		Year  int `json:"year"`
		Day   int `json:"day"`
		Month int `json:"month"`
	} `json:"dob"`
	Connect int    `json:"connect"`
	Version int    `json:"version"`
	S3      string `json:"s3"`
	Icon    string `json:"icon"`
	TnKey   string `json:"tn_key"`
}
