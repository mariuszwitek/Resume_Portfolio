/*-----------------------------------
*			File Decoder
* In maindaq-decoder.c, the function submit_all_data calls submit_*_data which calls uploadTDC or uploadScaler from this program.
*-------------------------------------*/

//INCLUDES
#include "Uploader.cpp"

using namespace std;
using namespace sql;

int uploadScaler(char* cPath, char* cServer, char* cSchema,
                 long int hitID, RawScalerStorage* scalerStorage)
{
    string path = string(cPath);
    string server = string(cServer);
    string schema = string(cSchema);
    
    int port = XXXX;
    if (server == "XXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    {
        port = XXXX;
    }

    //1 user
    //2 password
    //3 HODOINFO
    //4 CHAMBERINFO
    //5 RT
    //6 HODOMASK
    //7 TRIGGERROADS
    //8 TRIGGERINFO
    //9 SCALERINFO
    //vector<string> settingName = {"user", "password", "HODOMASK", "RT", "CHAMBERINFO",
    //                              "HODOINFO", "TRIGGERROADS", "TRIGGERINFO", "SCALERINFO"};
    vector<string> queryString;
    string user = "XXXXXXXXXX";
    string password = "XXXXXXXXXXXX";
    queryString.push_back("SELECT detectorName, elementID, rocID, boardID, "
                          "channelID, tPeak, width FROM hodoInfo");
    queryString.push_back("SELECT detectorName, elementID, rocID, boardID, "
                          "channelID, t0, offset, width FROM chamberInfo");
    queryString.push_back("SELECT detectorName, TRUNCATE(driftTime,1) AS `driftTime`, "
                          "driftDistance, TRUNCATE(resolution,2) AS `resolution`FROM RT");
    queryString.push_back("SELECT CONCAT_WS('_',hodoDetectorName, "
        "CAST(hodoElementID AS CHAR(5))) AS `hodo`, wireDetectorName, "
        "MIN(wireElementID) AS `minwire`, MAX(wireElementID) AS `maxwire` "
        "FROM HodoMask GROUP BY stationID, hodoDetectorName, hodoElementID, "
        "wireDetectorID, wireDetectorName "
        "ORDER BY hodoDetectorName, hodoElementID, wireDetectorName");
    queryString.push_back("SELECT roadID, detectorHalf, H1, H2, H3, H4 FROM TriggerRoads");
    queryString.push_back("SELECT detectorName, elementID, triggerLevel, rocID, boardID, "
                          "channelID, tPeak, width FROM triggerInfo");
    queryString.push_back("SELECT rocID, boardID, channelID, scalerName FROM scalerInfo");

    //create Uploader object
    Uploader* UP = new Uploader();

    //start timing
    clock_t t = clock();

    try
    {
        Driver *driver;
        Connection *con;
        Statement *stmt;
        ConnectOptionsMap opts;

        //connection information
        opts["hostName"]=server;
        opts["userName"]=user;
        opts["password"]=password;
        opts["schema"]=schema;
        opts["port"]=port;
        opts["OPT_LOCAL_INFILE"]=1;

        /* Create a connection */
        driver = get_driver_instance();
        con = driver->connect(opts);
        stmt = con->createStatement();

        ResultSet *res = stmt->executeQuery(*(prev(queryString.end())));

        //pass on result set and appNum = 6 to initialize upload for scaler data
        if (UP->initialize(res,6))
        {
            cerr<<"Problem initializing scalerInfo"<<endl;
            delete res;
            delete UP;
            delete stmt;
            delete con;
            return 1;
        }
        // cout<<"Decoding..."<<endl;
        //most important function decode* does most the work
        if (UP->decodeScaler(path, server, schema, stmt, hitID, scalerStorage))
        {
            cerr<<"Decoding hits file error"<<endl;
            delete res;
            delete UP;
            delete stmt;
            delete con;
            return 1;
        }
        delete res;
        delete UP;
        delete stmt;
        delete con;
    }
    catch (SQLException &e)
    {
        cout << "# ERR: SQLException in " << __FILE__;
        cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
        cout << "# ERR: " << e.what();
        cout << " (MySQL error code: " << e.getErrorCode();
        cout << ", SQLState: " << e.getSQLState() << " )" << endl;
        return 1;
    }
    t = clock() - t;
    // cout<<"Finished uploading run"<<endl;
    // printf("It took me %f seconds to run.\n",((float)t)/CLOCKS_PER_SEC);
    return 0;
}

int uploadTDC(char* cPath, char* cServer, char* cSchema,
              long int hitID, int runNum, int spillID, RawHitStorage* rawStorage,
              int nohit)
{
    //begining is the same as uploadScaler
    //setup db connection

    string path = string(cPath);
    string server = string(cServer);
    string schema = string(cSchema);

    InitMap();

    int port = XXXX;
    if (server == "XXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    {
        port = XXXX;
    }

    //1 user
    //2 password
    //3 HODOINFO
    //4 CHAMBERINFO
    //5 RT
    //6 HODOMASK
    //7 TRIGGERROADS
    //8 TRIGGERINFO
    //9 SCALERINFO
    vector<string> queryString;
    string user = "production";
    string password = "qqbar2mu+mu-";
    queryString.push_back("SELECT detectorName, elementID, rocID, boardID, "
                          "channelID, tPeak, width FROM hodoInfo");
    queryString.push_back("SELECT detectorName, elementID, rocID, boardID, "
                          "channelID, t0, offset, width FROM chamberInfo");
    queryString.push_back("SELECT detectorName, TRUNCATE(driftTime,1) AS `driftTime`, "
                          "driftDistance, TRUNCATE(resolution,2) AS `resolution`FROM RT");
    queryString.push_back("SELECT CONCAT_WS('_',hodoDetectorName, "
        "CAST(hodoElementID AS CHAR(5))) AS `hodo`, wireDetectorName, "
        "MIN(wireElementID) AS `minwire`, MAX(wireElementID) AS `maxwire` "
        "FROM HodoMask GROUP BY stationID, hodoDetectorName, hodoElementID, "
        "wireDetectorID, wireDetectorName "
        "ORDER BY hodoDetectorName, hodoElementID, wireDetectorName");
    queryString.push_back("SELECT roadID, detectorHalf, H1, H2, H3, H4 FROM TriggerRoads");
    queryString.push_back("SELECT detectorName, elementID, triggerLevel, rocID, boardID, "
                          "channelID, tPeak, width FROM triggerInfo");
    queryString.push_back("SELECT rocID, boardID, channelID, scalerName FROM scalerInfo");
    //create Uploader object
    Uploader* UP = new Uploader();

    //start clock
    clock_t t = clock();

    try
    {
        Driver *driver;
        Connection *con;
        Statement *stmt;
        ConnectOptionsMap opts;

        //connnection information
        opts["hostName"]=server;
        opts["userName"]=user;
        opts["password"]=password;
        opts["schema"]=schema;
        opts["port"]=port;
        opts["OPT_LOCAL_INFILE"]=1;

        /* Create a connection */
        driver = get_driver_instance();
        con = driver->connect(opts);
        stmt = con->createStatement();

        vector<ResultSet *> allResultSets;
        int appNum = 0;
        for (auto it = queryString.begin(); it != prev(queryString.end()); ++it)
        {
            ResultSet *res = stmt->executeQuery(*it);
            //initialize by passing in result sets with associated app number to initialize. Omit scalerInfo.
            if (UP->initialize(res,appNum))
            {
                cerr<<"Problem initializing app number "<<appNum<<endl;
                for (auto it = allResultSets.begin(); it != allResultSets.end(); ++it)
                {
                    delete *it;
                }
                delete UP;
                delete stmt;
                delete con;
                return 1;
            }
            allResultSets.push_back(res);
            appNum++;
        }

        // cout<<"Decoding..."<<endl;
        //Most of the heavy work is done in decodeTDC
        if (UP->decodeTDC(path, server, schema, stmt, hitID, runNum, spillID, rawStorage, nohit))
        {
            cerr<<"Decoding hits file error"<<endl;
            for (auto it = allResultSets.begin(); it != allResultSets.end(); ++it)
            {
                delete *it;
            }
            delete UP;
            delete stmt;
            delete con;
            return 1;
        }

        //cleanup
        for (auto it = allResultSets.begin(); it != allResultSets.end(); ++it)
        {
            delete *it;
        }
        delete UP;
        delete stmt;
        delete con;
    }
    catch (SQLException &e)
    {
        cout << "# ERR: SQLException in " << __FILE__;
        cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
        cout << "# ERR: " << e.what();
        cout << " (MySQL error code: " << e.getErrorCode();
        cout << ", SQLState: " << e.getSQLState() << " )" << endl;
        //delete correct things!
        return 1;
    }
    t = clock() - t;
    // cout<<"Finished uploading run"<<endl;
    // printf("It took me %f seconds to run.\n",((float)t)/CLOCKS_PER_SEC);
    return 0;
}

