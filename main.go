package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"bitbucket.org/intxlog/profiler/db"
	"bitbucket.org/intxlog/profiler/profiler"
)

func main() {
	test()
}

func test() {
	fmt.Printf("Starting profile...\n")
	start := time.Now()
	connStr := os.Args[1]
	t := db.NewPostgresConn(connStr)

	pConnStr := os.Args[2]
	pConn := db.NewPostgresConn(pConnStr)

	p := profiler.NewProfiler(t, pConn)

	profile := profiler.ProfileDefinition{
		FullProfileTables: []string{"loads"},
		CustomProfileTables: []profiler.TableDefinition{
			profiler.TableDefinition{
				TableName: "loads",
				Columns:   []string{"*"},
				CustomColumns: []profiler.CustomColumnDefition{
					profiler.CustomColumnDefition{
						ColumnName:       "tripmilesmin",
						ColumnDefinition: "min(tripmiles)",
					},
				},
			},
		},
	}
	// err := p.ProfileTables([]string{"consignees", "drops", "expenses", "loads", "appayments", "accountingexception", "acctlogs", "arinvoices", "activity_datas", "actiontypes", "apadjustments", "apissues", "claims", "notes", "api_users", "apledger", "aradjustment_applications", "aradjustments", "arinvoicesadjustments", "arissues", "arledger", "arpayments", "billogs", "assistants", "broker_teams", "call_logs", "usergoals", "commissionentrytypes", "character_entity", "carriersaferinfo", "carrierfallout", "carrierratings", "carrierpersonalnoload", "carriertrailers", "comchecks", "comcheckseedcodes", "bwusers", "checkcallstatus", "dispatchers", "docname2categories", "customercollections", "custportal_order_items", "custportal_orders", "dashboardlocations", "dbsignature", "commissions", "department_heads", "commissions_lpfexcludelog", "contacts", "department_managers", "departments", "documentcategories", "documentfolders", "eiafuelprices", "hideactivecolumns", "dropdatefix", "email_types", "employee2user", "glaccounts", "emails", "documentcmdqueue", "documenttypes", "equipmenttypes", "expensetypes", "glaccountmodules", "invoicecreation", "hidefuturecolumns", "insurancetypes", "locations", "keys", "loadatownrisklog", "insurancelistedas", "hidetodayscolumns", "syslogs", "incomingsms", "loadlocations", "lccarriertracker", "intacctclosedbooks", "loadprocessingfeeconfig", "macropoint_alerts", "macropoint_events", "macropoint_orderstatus", "macropointupdates", "oauth_access_tokens", "oauth_auth_codes", "oauth_clients", "oauth_personal_access_clients", "oauth_refresh_tokens", "loginattempts", "packettypes", "macropointglentries", "notesdomaintypes", "notetypes", "password_resets", "pickups", "positionlevels", "postings", "positivepayfiles", "ref_departments_positions", "ref_loads_macropoint", "ref_locations_brokerteams", "ref_user_department_heads", "phonenumbers", "ref_user_managers", "phonenumber_types", "payment_types", "permissions", "schema_version", "settings", "smslogs", "states", "teams", "rel_teams_sales_managers", "ref_users_emails", "ref_user_teams", "ref_users_phonenumbers", "ref_users_trainings", "salesmanagers", "rel_user_location", "stupidbrokerdidntprovideavalidphonenumbernotifications", "security", "tempinvupdate", "useractions", "todos", "user_devices", "userchangetypes", "usercounts", "usersentemails", "usersettings", "trailersizes", "user_hours", "trainings", "usersmainpage", "user_payments", "zipqueries", "verificationhashes", "zipcodes", "commodities", "inscustomertypes", "broker_lc", "packetssent", "carrier_commodity_exclusions", "advancetypes", "glaccounts2modules", "carrierchanges", "specialnotes", "commissionentries2arpayments", "advances", "positions", "checkcalls", "notesdomaincategorytypes", "billinginfo", "insurancerequests", "apbills", "manualcommissionlog", "commissionentries", "factoringcompanies", "customers", "ratetypes", "trailertypes", "ref_user_department_position", "rel_teams_captains", "rel_teams_managers", "userchangelog", "documents", "paytruck", "arterms", "country", "creditrequests", "drivers", "fuelsurcharges", "paytermtypes", "phonelogs", "securitygroups", "users", "carrier_setup_source_types", "carriers"})
	err := p.RunProfile(profile)

	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("Success")
	}

	end := time.Now()
	fmt.Printf("Finished... time taken: %v", end.Sub(start))
}
