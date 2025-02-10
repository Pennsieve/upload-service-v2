package pkg

type AWSRegions struct {
	FullName   string
	RegionCode string
}

// Regions as of Feb 2025
var Regions = map[string]AWSRegions{
	"use1":  {"US East (N. Virginia)", "us-east-1"},
	"use2":  {"US East (Ohio)", "us-east-2"},
	"usw1":  {"US West (N. California)", "us-west-1"},
	"usw2":  {"US West (Oregon)", "us-west-2"},
	"afs1":  {"Africa (Cape Town)", "af-south-1"},
	"ape1":  {"Asia Pacific (Hong Kong)", "ap-east-1"},
	"aps2":  {"Asia Pacific (Hyderabad)", "ap-south-2"},
	"apse3": {"Asia Pacific (Jakarta)", "ap-southeast-3"},
	"apse5": {"Asia Pacific (Malaysia)", "ap-southeast-5"},
	"apse4": {"Asia Pacific (Melbourne)", "ap-southeast-4"},
	"aps1":  {"Asia Pacific (Mumbai)", "ap-south-1"},
	"apne3": {"Asia Pacific (Osaka)", "ap-northeast-3"},
	"apne2": {"Asia Pacific (Seoul)", "ap-northeast-2"},
	"apse1": {"Asia Pacific (Singapore)", "ap-southeast-1"},
	"apse2": {"Asia Pacific (Sydney)", "ap-southeast-2"},
	"apse7": {"Asia Pacific (Thailand)", "ap-southeast-7"},
	"apne1": {"Asia Pacific (Tokyo)", "ap-northeast-1"},
	"cac1":  {"Canada (Central)", "ca-central-1"},
	"caw1":  {"Canada West (Calgary)", "ca-west-1"},
	"euc1":  {"Europe (Frankfurt)", "eu-central-1"},
	"euw1":  {"Europe (Ireland)", "eu-west-1"},
	"euw2":  {"Europe (London)", "eu-west-2"},
	"eus1":  {"Europe (Milan)", "eu-south-1"},
	"euw3":  {"Europe (Paris)", "eu-west-3"},
	"eus2":  {"Europe (Spain)", "eu-south-2"},
	"eun1":  {"Europe (Stockholm)", "eu-north-1"},
	"euc2":  {"Europe (Zurich)", "eu-central-2"},
	"ilc1":  {"Israel (Tel Aviv)", "il-central-1"},
	"mxc1":  {"Mexico (Central)", "mx-central-1"},
	"mes1":  {"Middle East (Bahrain)", "me-south-1"},
	"mec1":  {"Middle East (UAE)", "me-central-1"},
	"sae1":  {"South America (São Paulo)", "sa-east-1"},
	"usge1": {"AWS GovCloud (US-East)", "us-gov-east-1"},
	"usgw1": {"AWS GovCloud (US-West)", "us-gov-west-1"},
}

func getKeys(myMap map[string]AWSRegions) []string {
	keys := make([]string, 0, len(myMap))
	for key := range myMap {
		keys = append(keys, key)
	}
	return keys
}
