{
    "General" :
    {
        "TimeSteps" : 0,
        "Interval" : 0.200,
        "SecondsPerStep" : 2.0,
        "StartTimeOfDay" : 7.0,
        "MaximumTravelers" : 1600,
		"WorldInfoFile" : "worldinfo.js",
		"Data" : "jsondata",
		"Store" : "SimpleStore",
		"MultiProcessing" : false,
		"Timer" :
		{
			"Seconds" : 0,
			"Minutes" : 1,
			"Hours" : 0
		},
        "Connectors" : ["social", "sumo", "opensim"]
    },

    "Builder" :
    {
        "ExtensionFiles" : ["networks/fullnet/layout.py", "networks/fullnet/business.py", "networks/fullnet/people.py"]
    },

    "SocialConnector" :
    {
        "WaitMean" : 1000.0,
        "WaitSigma" : 200.0,
        "PeopleCount" : 120
    },

    "OpenSimConnector" :
    {
        "WorldSize" : [810.0, 810.0, 100.0],
        "RegionSize" : [1024.0, 1024.0],
        "BuildOffset" : [415.0, 415.0],
        "WorldOffset" : [10.0, 10.0, 25.5],
        "Scale" : 0.4,
        "PositionDelta" : 0.1,
        "VelocityDelta" : 0.1,
        "AccelerationDelta" : 0.05,
        "UpdateThreadCount" : 1,
        "Interval" : 0.2,
        "Binary" : true,
	"Scenes" :
	{
		"City00" :
		{
			"Name" : "City00",
			"AvatarName" : "Test User",
			"Password" : "test",
			"EndPoint" : "http://parana.mdg.lab:9000/Dispatcher/",
			"Location" : [0,0]
		},
		"City01" :
		{
			"Name" : "City01",
			"AvatarName" : "Test User",
			"Password" : "test",
			"EndPoint" : "http://ganges.mdg.lab:9000/Dispatcher/",
			"Location" : [0,1]
		},
		"City10" :
		{
			"Name" : "City10",
			"AvatarName" : "Test User",
			"Password" : "test",
			"EndPoint" : "http://rhine.mdg.lab:9000/Dispatcher/",
			"Location" : [1,0]
		},
		"City11" :
		{
			"Name" : "City11",
			"AvatarName" : "Test User",
			"Password" : "test",
			"EndPoint" : "http://hudson.mdg.lab:9000/Dispatcher/",
			"Location" : [1,1]
		}
	}
    },
    
    "SumoConnector" :
    {
	"SumoNetworkPath" : "networks/fullnet/net/",
	"SumoDataFilePrefix" : "network",
        "NetworkScaleFactor" : 10.0,
        "VehicleScaleFactor" : 4.0,
        "ConfigFile" : "networks/fullnet/fullnet.sumocfg",
        "ExtensionFiles" : [ ],
        "VelocityFudgeFactor" : 1.0,
        "SumoPort" : 8813
    },

    "StatsConnector" :
    {
        "CollectObjectDynamics" : true,
        "CollectObjectPattern" : "worker[0123456789]+_trip.*"
    },

    "RoadTypes" :
    [
	{
	    "Name" : "Universal Road Segment",
	    "RoadTypes" : [ "etype1A", "etype1B", "etype1C",
                            "etype2A", "etype2B", "etype2C",
                            "etype3A", "etype3B", "etype3C",
                            "parking_entry", "driveway_road", "parking_drive",
                            "1way2lane", "1way3lane" ],
	    "ZOffset" : 20.5,
	    "AssetID" : { "ObjectName" : "SumoAssets Edges", "ItemName" : "Universal Road Segment" }
	}

    ],

    "IntersectionTypes" :
    [
	{
	    "Name" : "Universal Intersection [* * * *]",
	    "AssetID" : { "ObjectName" : "SumoAssets Nodes", "ItemName" : "Universal Intersection [* * * *]" },
	    "ZOffset" : 20.50,
	    "Padding" : 0.0,
	    "Signature" : ["*/*", "*/*", "*/*", "*/*"],
            "IntersectionTypes" : [ "driveway_node", "parking_drive_intersection", "apartment", "business", "townhouse", "stoplight", "priority" ]
	}
    ],

    "Cities" :
    [
        {
            "Name" : "City00",
            "Offset" : [0,0]
        },

        {
            "Name" : "City11",
            "Offset" : [1100,1100]
        },
	{
	    "Name" : "City10",
	    "Offset" : [1100,0]
	},
	{
	    "Name" : "City01",
	    "Offset" : [0,1100]
	}
    ],

    "CityConnections" :
    [
	["City00:main200W400N","City01:main200W400S"],
	["City00:main400E100N","City10:main400W100N"],
	["City01:main400E100N","City11:main400W100N"],
	["City10:main200W400N","City11:main200W400S"]
    ]
}
