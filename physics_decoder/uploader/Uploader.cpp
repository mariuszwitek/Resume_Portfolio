#include <iomanip>
#include "Uploader.h"

Uploader::Uploader()
{
    roadset = new RoadSet();
    HIC_Map = new unordered_map<unsigned long, vector<string>>;
    SI_Map = new unordered_map<unsigned long, string>;
    TI_Map = new unordered_map<unsigned long, vector<string>>;
    RT_Map = new unordered_map<string, vector<string>>;
    HM_Map = new unordered_map< string, vector< tuple< string, int, int>>>;
    T4 = new vector< tuple<int,vector< tuple<int,vector< tuple<int,vector< tuple<int,string>>*>>*>>*>>;
    B4 = new vector< tuple<int,vector< tuple<int,vector< tuple<int,vector< tuple<int,string>>*>>*>>*>>;
    original_pulse = new unordered_map< string, pair< double, int > >;
    use_hits_for_masking = 0;
}

Uploader::~Uploader()
{
    delete HIC_Map;
    HIC_Map = nullptr;
    delete TI_Map;
    TI_Map = nullptr;
    delete SI_Map;
    SI_Map = nullptr;
    delete RT_Map;
    RT_Map = nullptr;
    delete HM_Map;
    HM_Map = nullptr;
    deleteTrigger();
}

//has to be near top or at least before it first call
bool sortRF(tuple<int,double> a, tuple<int,double> b) 
{
    return (get<0>(a) < get<0>(b));
}

string Uploader::toString(vector<string> &input)
{
    /*
    final order of lines

    hitInfo
    ==========
    0 hitID
    1 spillID
    2 eventID
    3 detectorName
    4 elementID
    5 tdcTime
    6 inTime
    7 driftTime
    8 driftDistance
    9 resolution
    10 masked
    11 dataQuality

    triggerHitInfo
    ==============
    0 hitID
    1 spillID
    2 eventID
    3 detectorName
    4 elementID
    5 triggerLevel
    6 tdcTime
    7 inTime
    8 errorBits
    9 dataQuality
    */

    if (input.size() == 12) //hit needs to be reordered
    {
        iter_swap(input.begin()+7,input.begin()+10);
        iter_swap(input.begin()+8,input.begin()+10);
        iter_swap(input.begin()+9,input.begin()+10);
    }

    /*
    new hitInfo order
    ==========
    0 hitID
    1 spillID
    2 eventID
    3 detectorName
    4 elementID
    5 tdcTime
    6 inTime
    7 masked
    8 driftTime
    9 driftDistance
    10 resolution
    11 dataQuality
    */

    // ROOT FILE HANDLING
    Hit h;

    if (input.size() == 12) // Hits
    {
        h.detectorID    = getDetectorID(input.at(3));
        h.elementID     = atoi(input.at(4).c_str());
        h.tdcTime       = atof(toStrMaxDecimals(input.at(5), 3).c_str());
        h.index         = atoi(input.at(0).c_str());
        h.setInTime (atoi (input.at(6).c_str() ) );
        h.setHodoMask (atoi (input.at(7).c_str() ) );
        h.setTriggerMask (!((atoi (input.at(11).c_str() ) ) & 4));
        h.setOriginalInTimePulse (!((atoi (input.at(11).c_str() ) ) & 8));
        //h.inTime_flag = (atoi (input.at(6).c_str() ) );
        //h.hodoMask_flag = (atoi (input.at(7).c_str() ) );
        //h.triggerMask_flag = !((atoi (input.at(11).c_str() ) ) & 4);
        //h.originalInTimePulse_flag = !((atoi (input.at(11).c_str() ) ) & 8);
        h.driftDistance = atof(toStrMaxDecimals(input.at(9), 4).c_str());

        rawEventBuffer[atoi(input.at(2).c_str())].insertHit(h);

    } else if (input.size() == 10) { // TriggerHits

        h.detectorID    = getDetectorID(input.at(3));
        h.elementID     = atoi(input.at(4).c_str());
        h.tdcTime       = atof(toStrMaxDecimals(input.at(6), 3).c_str());
        h.index         = atoi(input.at(0).c_str());
        h.setInTime(atoi(input.at(7).c_str()));
        //h.inTime_flag = atoi(input.at(7).c_str());

        rawEventBuffer[atoi(input.at(2).c_str())].insertTriggerHit(h);
    }
    // --------------------

    stringstream ss;
    for (vector<string>::iterator it = input.begin(); it != --input.end(); ++it)
    {
        ss<<*(it)<<'\t';
    }
    ss<<input.back()<<"\n";

    return ss.str();
}

unsigned long Uploader::HIC_HF(string &a, string &b, string &c)
{
    unsigned short d = atoi(a.data());
    unsigned short e = atoi(b.data());
    unsigned short f = atoi(c.data());
    unsigned long out = d;
    out <<= 16;
    out |= e;
    out <<= 16;
    out |= f;
    return out;
}

unsigned long Uploader::SI_HF(string &a, string &b, string &c)
{
    return HIC_HF(a,b,c);
}

unsigned long Uploader::TI_HF(string &a, string &b, string &c)
{
    return HIC_HF(a,b,c);
}

string Uploader::RT_HF(string &a, string &b)
{
    return (a + "_" + b);
}

string Uploader::HM_HF(string a, string b)
{
    return (a.substr(0,3) + "_" + b); //possibly chop off first H
}

//app_name app_number
//===================
//HODO_INFO	    0
//CHAMBER_INFO  1
//RT		    2
//HODOMASK	    3
//TRIGGER_ROADS	4
//TRIGGER_INFO	5
//SCALER_INFO	6

int Uploader::initialize(ResultSet *res, int app)
{
    //expected header information
    vector<string> headerFields;
    if (app == 0)
    {
        headerFields = {"rocID", "boardID", "channelID", "detectorName", "elementID", 
                        "tPeak", "width"};
    }
    else if (app == 1)
    {
        headerFields = {"rocID", "boardID", "channelID", "detectorName", "elementID",
                        "t0", "offset", "width"};
    }
    else if (app == 2)
    {
        headerFields = {"detectorName", "driftTime", "driftDistance", "resolution"};
    }
    else if (app == 3)
    {
        headerFields = {"hodo", "wireDetectorName", "minwire", "maxwire"};
    }
    else if (app == 4)
    {
        headerFields = {"roadID", "detectorHalf", "H1", "H2", "H3", "H4"};
    }
    else if (app == 5)
    {
        headerFields = {"rocID", "boardID", "channelID", "detectorName", "elementID",
            "triggerLevel", "tPeak", "width"};
    }
    else if (app == 6)
    {
        headerFields = {"rocID", "boardID", "channelID", "scalerName"};
    }
    else
    {
        cerr << "Internal error" << endl;
        return 1;
    }

    vector<int> index;
    for (auto it = headerFields.begin(); it != headerFields.end(); ++it)
    {
        index.push_back(res->findColumn(*it)); //throws mysql exception if fails
    }

    //hodomask helpers
    string prevHodo = "";
    vector< tuple< string, int, int>> block;

    while (res->next())
    {
        if (app == 0 || app == 1)
        {
            string rocID = res->getString(index[0]);
            string boardID = res->getString(index[1]);
            string channelID = res->getString(index[2]);
            unsigned long key = HIC_HF(rocID, boardID, channelID);
            vector<string> value;
            if (app == 0)
            {
                value = {to_string(static_cast<long long>(app)), res->getString(index[3]), res->getString(index[4]),
                    res->getString(index[5]), res->getString(index[6])};
            }
            else if (app == 1)
            {
                value = {to_string(static_cast<long long>(app)), res->getString(index[3]), res->getString(index[4]),
                    res->getString(index[5]), res->getString(index[6]), res->getString(index[7])};
            }
            (*HIC_Map)[key] = value;
        }
        else if (app == 2)
        {
            string k1 = res->getString(index[0]);
            string k2 = res->getString(index[1]);
            string key = RT_HF(k1,k2);
            vector<string> value = {res->getString(index[2]),res->getString(index[3])};
            (*RT_Map)[key] = value;
        }
        else if (app == 3)
        {
            //hodomask
            string curHodo = res->getString(index[0]);
            if (prevHodo == curHodo)
            {
                block.emplace_back(res->getString(index[1]), res->getInt(index[2]),
                        res->getInt(index[3]));
            }
            else
            {
                if (!block.empty())
                {
                    (*HM_Map)[prevHodo] = block;
                    prevHodo = curHodo;
                    block.clear();
                }
                prevHodo = curHodo;
                block.emplace_back(res->getString(index[1]), res->getInt(index[2]),
                        res->getInt(index[3]));
            }
        }
        else if (app == 4)
        {
            //triggerRoads mapping
            string roadID = res->getString(index[0]);
            string detectorHalf = res->getString(index[1]);
            int H1 = res->getInt(index[2]);
            int H2 = res->getInt(index[3]);
            int H3 = res->getInt(index[4]);
            int H4 = res->getInt(index[5]);

            roadset->insert_road(stoi(roadID), detectorHalf, H1, H2, H3, H4);

            vector< tuple< int, vector< tuple< int, vector< tuple< int, vector< tuple<int, string>>*> >*> >*> >* M4;
            if (detectorHalf == "T")
            {
                M4 = T4;
            }
            else
            {
                M4 = B4;
            }

            bool foundH4 = false;
            tuple<int, string> H1Val = make_tuple(H1, roadID);
            for (auto it = M4->begin(); it != M4->end(); ++it)
            {
                if (get<0>(*it) == H4)
                {
                    foundH4 = true;
                    bool foundH3 = false;
                    auto h3s = get<1>(*it);
                    for (auto itt = h3s->begin(); itt != h3s->end(); ++itt)
                    {
                        if (get<0>(*itt) == H3)
                        {
                            foundH3 = true;
                            bool foundH2 = false;
                            auto h2s = get<1>(*itt);
                            for (auto ittt = h2s->begin(); ittt != h2s->end(); ++ittt)
                            {
                                if (get<0>(*ittt) == H2)
                                {
                                    foundH2 = true;
                                    get<1>(*ittt)->push_back(H1Val);
                                    break;
                                }
                            }
                            if (!foundH2)
                            {
                                vector< tuple<int,string>>* VecH1Val = 
                                    new vector< tuple<int,string>>; //combine
                                VecH1Val->push_back(H1Val);
                                tuple<int,vector< tuple<int,string>>*> H2H1Val = 
                                    make_tuple(H2, VecH1Val);
                                h2s->push_back(H2H1Val);
                            }
                            break;
                        }
                    }
                    if (!foundH3)
                    {
                        vector< tuple<int,string>>* VecH1Val = new vector< tuple<int,string>>; //combine
                        VecH1Val->push_back(H1Val);
                        tuple<int,vector< tuple<int,string>>*> H2H1Val = make_tuple(H2, VecH1Val);
                        vector< tuple<int,vector< tuple<int,string>>*>>* VecH2H1Val = 
                            new vector< tuple<int,vector< tuple<int,string>>* >>;
                        VecH2H1Val->push_back(H2H1Val);
                        tuple< int,vector< tuple<int,vector< tuple<int,string>>*>>*> H3H2H1Val = 
                            make_tuple(H3, VecH2H1Val);
                        h3s->push_back(H3H2H1Val);
                    }
                    break;
                }
            }
            if (!foundH4)
            {
                vector< tuple<int,string>>* VecH1Val = new vector< tuple<int,string>>; //combine

                VecH1Val->push_back(H1Val);

                tuple<int,vector< tuple<int,string>>*> H2H1Val = make_tuple(H2, VecH1Val);

                vector< tuple<int,vector< tuple<int,string>>*>>* VecH2H1Val = 
                    new vector< tuple<int,vector< tuple<int,string>>* >>;

                VecH2H1Val->push_back(H2H1Val);

                tuple< int,vector< tuple<int,vector< tuple<int,string>>*>>*> H3H2H1Val = 
                    make_tuple(H3, VecH2H1Val);

                vector< tuple<int,vector< tuple<int,vector< tuple<int,string>>*>>*>>* VecH3H2H1Val = 
                    new vector< tuple<int,vector< tuple<int,vector< tuple<int,string>>*>>*>>;

                VecH3H2H1Val->push_back(H3H2H1Val);

                tuple<int,vector< tuple<int,vector< tuple<int,vector< tuple<int,string>>*>>*>>*> H4H3H2H1Val = make_tuple(H4, VecH3H2H1Val);

                M4->push_back(H4H3H2H1Val);
            }
        }
        else if (app == 5)
        {
            string rocID = res->getString(index[0]);
            string boardID = res->getString(index[1]);
            string channelID = res->getString(index[2]);
            string detectorName = res->getString(index[3]);
            unsigned long key = TI_HF(rocID, boardID, channelID);
            vector<string> value = {"0", res->getString(index[3]), res->getString(index[4]),
                                    res->getString(index[5]), res->getString(index[6]),
                                    res->getString(index[7])};
            (*TI_Map)[key] = value;
        }
        else if (app == 6)
        {
            string rocID = res->getString(index[0]);
            string boardID = res->getString(index[1]);
            string channelID = res->getString(index[2]);
            unsigned long key = SI_HF(rocID, boardID, channelID);
            (*SI_Map)[key] = res->getString(index[3]);
        }
    }
    if (app == 3)
    {
        //get last hodo
        (*HM_Map)[prevHodo] = block;
    }
    return 0;
}

int Uploader::decodeScaler(string path, string server, string schema, 
                           Statement *stmt, long int hitID, 
                           RawScalerStorage* scalerStorage)
{
    //datastructure
    string type = "scaler";
    vector<string> badEvents;
    string temp1 = path + "scaler-"+server+"-"+schema+".out";
    const char* scalerFileName = temp1.data();
    ofstream scalerFile(scalerFileName);
    for (int k = 0; k < scalerStorage->scalerCount; k++)
    {
        string scalerID = to_string(static_cast<unsigned long long>(hitID));
        string spillID = to_string(static_cast<unsigned long long>(scalerStorage->scalerSpillID[k]));
        string spillType = to_string(static_cast<unsigned long long>(scalerStorage->scalerSpillType[k]));
        string rocID = to_string(static_cast<unsigned long long>(scalerStorage->scalerROC[k]));
        string boardID = to_string(static_cast<unsigned long long>(scalerStorage->scalerBoardID[k]));
        string channelID = to_string(static_cast<unsigned long long>(scalerStorage->scalerChannelID[k]));
        string value = to_string(static_cast<unsigned long long>(scalerStorage->scalerValue[k]));
        string dataQuality = "0";

        string scalerName = (*SI_Map)[SI_HF(rocID, boardID, channelID)];
        if (scalerName.empty())
        {
            scalerName = "\\N";
        }
        vector<string> scalerInfo = {scalerID, spillID, spillType,
                                     scalerName, value, 
                                     dataQuality};
        string outLine = toString(scalerInfo);
        if (k == scalerStorage->scalerCount-1)
        {
            outLine = outLine.substr(0, outLine.size()-1);
        }
        scalerFile.write(outLine.data(), outLine.size());
        hitID++;
    }
    scalerFile.close();
    // cout<<"Uploading Scaler table"<<endl;
    string loadQuery = "LOAD DATA LOCAL INFILE '" + string(scalerFileName) +
                       "' INTO TABLE " + schema + ".Scaler";
    stmt->execute(loadQuery);
    // cout<<"Cleaning up"<<endl;
    string rmFile = "rm " + string(scalerFileName);
    system(rmFile.data());
    return 0;
}


int Uploader::decodeTDC(string path, string server, string schema, Statement *stmt,
                        long int hitID, int runNum, int spillID, RawHitStorage* rawStorage,
                        int nohit)
{
    //if (runNum < USE_HITS_FOR_MASKING_BEFORE_THIS_RUN)
    if (runNum < 11795)
    {
        use_hits_for_masking = 1;
    }
   
    // Collect EventID's with no in-Time RF hit
    vector<string> badEvents;

    // The order in which the data is loaded into the 'wordBuffer' vector
    vector<string> headerFields = {"hitID", "spillID", "eventID", "rocID", "boardID", 
                                   "channelID", "tdcTime"};

    // File names. Can this be reduced a bit?
    string temp1 = path + "/hits-"+server+"-"+schema+".out";
    string temp2 = path + "/triggerHits-"+server+"-"+schema+".out";
    string temp3 = path + "/triggerRoads-"+server+"-"+schema+".out";
    string temp4 = path + "/itho-"+server+"-"+schema+".out";
    const char* hitsFileName = temp1.data();
    const char* triggerHitsFileName = temp2.data();
    const char* triggerRoadsFileName = temp3.data();
    const char* ithoFileName = temp4.data();
    ofstream hitsFile(hitsFileName);
    ofstream triggerHitsFile(triggerHitsFileName);
    ofstream triggerRoadsFile(triggerRoadsFileName);
    ofstream ithoFile(ithoFileName);

    // Stores the row of raw TDC data
    string wordBuffer[headerFields.size()];
    // Store the hit and t-hit processed data here
    vector<vector<string>> hitStorage;
    vector<vector<string>> triggerHitStorage;

    // Keep track of current eventID
    unsigned int cur_event;
   
    // Keep track of in-time hodoscope hits, used for masking
    vector<string> inTimeHodos;



    // Keep track of 
    vector<tuple<int,double>> inTimeRFs;
    vector<int> eventBoards;
    vector<vector<int>> BTriggerGroup; //order: B1 B2 B3 B4
    vector<vector<int>> TTriggerGroup; //order: T1 T2 T3 T4
    for (int i = 0; i < 4; i++)
    {
        vector<int> elemB;
        vector<int> elemT;
        BTriggerGroup.push_back(elemB);
        TTriggerGroup.push_back(elemT);
    }

    // Some values for hits processing
    string inTime;
    double tpeak;
    double width;
    double tdcTime;

    // Map a <detectorName, elementID> to a <tdcTime, array_index>
    // 1. if P% or D% Hit
    // 2. if inTime
    // 3. if does not exist a map entry, make one
    // 4. if does exist an entry, compare tdcTime
    // 5. if tdcTime > existing tdcTime, update the tdcTime and index
    // 6. after all hits for the event, mark the each hit at its index
    string det_el;
    pair<double, int> tdc_ind;

    // Get a vector of all eventID's from hits and triggerhits
    std::vector< int > event_array;
    event_array = rawStorage->get_events();

    // Keep track of where you are within each array of hit and t-hit data
    int cur_v1495 = 0;
    int cur_tdc = 0;

    // Begin looping through events!
    for (auto it = event_array.begin(); it != event_array.end(); ++it)
    {
        cur_event = *it; 
        /*
        wordBuffer
        ===========
        0 hitID
        1 spillID
        2 eventID
        3 rocID
        4 boardID
        5 channelID
        6 tdcTime

        hitInfo
        ==========
        0 hitID
        1 spillID
        2 eventID
        3 detectorName
        4 elementID
        5 tdcTime
        6 inTime
        7 driftTime
        8 driftDistance
        9 resolution
        10 masked
        11 dataQuality

        triggerHitInfo
        ==============
        0 hitID
        1 spillID
        2 eventID
        3 detectorName
        4 elementID
        5 triggerLevel
        6 tdcTime
        7 inTime
        8 errorBits
        9 dataQuality

        hashData
        =================
        0 app (-1 no match 0 hodo 1 chamber)

        hodo
        -----------
        1 detectorName
        2 elementID
        3 tpeak
        4 width

        chamber:
        -------------
        1 detectorName
        2 elementID
        3 t0
        4 offset
        5 width

        triggerData:
        =================
        0 detectorName
        1 elementID
        2 triggerLevel
        3 tPeak
        4 width
        */

        //---------------fill line----------------

        // TRIGGER HITS
        while (rawStorage->v1495EventID[cur_v1495] == cur_event)
        {
            wordBuffer[0] = to_string(static_cast<unsigned long long>(hitID));
            wordBuffer[1] = to_string(static_cast<unsigned long long>(rawStorage->v1495SpillID[cur_v1495]));
            wordBuffer[2] = to_string(static_cast<unsigned long long>(rawStorage->v1495EventID[cur_v1495]));
            wordBuffer[3] = to_string(static_cast<unsigned long long>(rawStorage->v1495ROC[cur_v1495]));
            wordBuffer[4] = to_string(static_cast<unsigned long long>(rawStorage->v1495BoardID[cur_v1495]));
            wordBuffer[5] = to_string(static_cast<unsigned long long>(rawStorage->v1495ChannelID[cur_v1495]));
            wordBuffer[6] = to_string(static_cast<long double>(rawStorage->v1495StopTime[cur_v1495]));
            
            vector<string> triggerHitInfo;
            bool RFpush = false;
            int boardID = atoi(wordBuffer[4].data());
            if (find(eventBoards.begin(), 
                     eventBoards.end(), boardID) == eventBoards.end())
            {
                eventBoards.push_back(boardID);
            }
            vector<string> triggerData = (*TI_Map)[TI_HF(wordBuffer[3], 
                                                         wordBuffer[4],
                                                         wordBuffer[5])];
            if (triggerData.empty())
            {
                triggerData.push_back("-1");
                for (int i = 0; i < 5; i++)
                {
                    triggerData.push_back("\\N");
                }
            }

            //universal
            //hitID
            triggerHitInfo.push_back(wordBuffer[0]);
            //spillID
            triggerHitInfo.push_back(wordBuffer[1]);
            //eventID
            triggerHitInfo.push_back(wordBuffer[2]);

            int app = atoi(triggerData[0].data());
            if (app == -1)
            {
                //no match found
                //detectorName
                triggerHitInfo.push_back("\\N");
                //elementID
                triggerHitInfo.push_back("\\N");
                //triggerLevel
                triggerHitInfo.push_back("\\N");
                //tdcTime
                triggerHitInfo.push_back(wordBuffer[6]);
                //boardID temp val
                triggerHitInfo.push_back(wordBuffer[4]);
                //tpeak temp val
                triggerHitInfo.push_back(triggerData[4]);
                //width temp val
                triggerHitInfo.push_back(triggerData[5]);
            }
            else if (app == 0)
            {
                //match found
                //detectorName
                triggerHitInfo.push_back(triggerData[1]);
                //elementID
                triggerHitInfo.push_back(triggerData[2]);
                //triggerLevel
                triggerHitInfo.push_back(triggerData[3]);
                //tdcTime
                triggerHitInfo.push_back(wordBuffer[6]);
                //boardID temp val
                triggerHitInfo.push_back(wordBuffer[4]);
                //if RF 8->inTime
                //else 8->tpeak 9->width
                if (triggerData[1] == "RF")
                {
                    if ((triggerData[4] != "\\N") && (triggerData[5] != "\\N") && 
                        (wordBuffer[6] != "\\N")) //should check for null tho, but not \\N?
                    {
                        tpeak = atof(triggerData[4].data());
                        width = atof(triggerData[5].data());
                        tdcTime = atof(wordBuffer[6].data());
                        if (abs(tdcTime-tpeak) <= .5 * width)
                        {
                            RFpush = true;
                            triggerHitInfo.push_back("1");
                        }
                        else
                        {
                            triggerHitInfo.push_back("0");
                        }
                    }
                    else
                    {
                        triggerHitInfo.push_back("0");
                    }
                }
                else
                {
                    //tpeak temp val
                    triggerHitInfo.push_back(triggerData[4]);
                    //width temp val
                    triggerHitInfo.push_back(triggerData[5]);
                }
            }

            if (RFpush)
            {
                inTimeRFs.emplace_back(boardID, tdcTime);
            }
            triggerHitStorage.push_back(triggerHitInfo);

            cur_v1495++;
            hitID++;
        }

        // HITS ============================================
        while (rawStorage->tdcEventID[cur_tdc] == cur_event)
        {
            wordBuffer[0] = to_string(static_cast<unsigned long long>(hitID));
            wordBuffer[1] = to_string(static_cast<unsigned long long>(rawStorage->tdcSpillID[cur_tdc]));
            wordBuffer[2] = to_string(static_cast<unsigned long long>(rawStorage->tdcEventID[cur_tdc]));
            wordBuffer[3] = to_string(static_cast<unsigned long long>(rawStorage->tdcROC[cur_tdc]));
            wordBuffer[4] = to_string(static_cast<unsigned long long>(rawStorage->tdcBoardID[cur_tdc]));
            wordBuffer[5] = to_string(static_cast<unsigned long long>(rawStorage->tdcChannelID[cur_tdc]));
            wordBuffer[6] = to_string(static_cast<long double>(rawStorage->tdcStopTime[cur_tdc]));

            inTime = "0"; 
            bool h1push = false; //H___ and intime
            vector<string> hashData = (*HIC_Map)[HIC_HF(wordBuffer[3], wordBuffer[4], wordBuffer[5])];

            if (hashData.empty())
            {
                hashData.push_back("-1");
                for (int i = 0; i < 4; i++)
                {
                    hashData.push_back("\\N");
                }
            }

            vector<string> hitInfo;

            //universal
            //hitID
            hitInfo.push_back(wordBuffer[0]);
            //spillID
            hitInfo.push_back(wordBuffer[1]);
            //eventID
            hitInfo.push_back(wordBuffer[2]);

            int app = atoi(hashData[0].data());
            if (app == -1)
            {
                //no match found

                //detectorName
                hitInfo.push_back("\\N");
                //elementID
                hitInfo.push_back("\\N");
                //tdcTime
                hitInfo.push_back(wordBuffer[6]);
                //inTime
                hitInfo.push_back("0");
                //driftTime
                hitInfo.push_back("\\N");
                //driftDistance
                hitInfo.push_back("\\N");
                //resolution
                hitInfo.push_back("\\N");
            }
            else if (app == 0)
            {
                // Hodoscope Match
                
                // Timing -----------
                if ((hashData[3] != "\\N") && (hashData[4] != "\\N") && (wordBuffer[6] != "\\N"))
                {
                    double tpeak = atof(hashData[3].data());
                    double width = atof(hashData[4].data());
                    double tdcTime = atof(wordBuffer[6].data());
                    if (abs(tdcTime-tpeak) <= .5 * width)
                    {
                        inTime = "1";
                        char thirdLet = hashData[1].data()[2];
                        if ((hashData[1].data()[0] == 'H') && ((thirdLet == 'B') || (thirdLet == 'T')))
                        {
                            h1push = true;
                        }
                    }
                    else
                    {
                        inTime = "0";
                    }
                }
                else
                {
                    inTime = "0";
                }

                //detectorName
                hitInfo.push_back(hashData[1]);
                //elementID
                hitInfo.push_back(hashData[2]);
                //tdcTime
                hitInfo.push_back(wordBuffer[6]);
                //inTime
                hitInfo.push_back(inTime);
                //driftTime
                hitInfo.push_back("\\N");
                //driftDistance
                hitInfo.push_back("\\N");
                //resolution
                hitInfo.push_back("\\N");
            }
            else if (app == 1)
            {
                //chamber match
                
                // Timing ----------------
                if ((hashData[3] != "\\N") && (hashData[4] != "\\N") && 
                    (hashData[5] != "\\N") && (wordBuffer[6] != "\\N"))
                {
                    double t0 = atof(hashData[3].data());
                    double off = atof(hashData[4].data());
                    double width = atof(hashData[5].data());
                    double tdcTime = atof(wordBuffer[6].data());
                    if ((tdcTime <= (t0 + off)) && (tdcTime >= (t0 + off - width)))
                    {
                        inTime = "1";
                    }
                    else
                    {
                        inTime = "0";
                    }
                }
                else
                {
                    inTime = "0";
                }

                // DriftTime ---------------
                string driftTime;
                if ((wordBuffer[6] != "\\N") && (hashData[3] != "\\N"))
                {
                    double tdcTime = atof(wordBuffer[6].data());
                    double t0 = atof(hashData[3].data());
                    if (tdcTime > t0)
                    {
                        driftTime = "0.0";
                    }
                    else
                    {
                        string temp = to_string(static_cast<long double>(2.5*(round((t0 - tdcTime)/2.5))));
                        driftTime = temp.substr(0, temp.find(".") + 2);
                    }
                }
                else
                {
                    driftTime = "\\N";
                }

                // Mapping -----------------

                //detectorName
                hitInfo.push_back(hashData[1]);
                //elementID
                hitInfo.push_back(hashData[2]);
                //tdcTime
                hitInfo.push_back(wordBuffer[6]);
                //inTime
                hitInfo.push_back(inTime);
                //driftTime
                hitInfo.push_back(driftTime);

                // RT Application ----------
                if (hashData[1] != "\\N" && driftTime != "\\N")
                {
                    string pCheck = hashData[1];
                    if (pCheck.at(0) == 'P')
                    {
                        pCheck = pCheck.substr(0,3);
                    }
                    //didn't use rHashFunction because already truncated driftTime
                    vector<string> rtData = (*RT_Map)[RT_HF(pCheck,driftTime)];
                    if (rtData.size() == 0) //didn't find anything
                    {
                        hitInfo.insert(hitInfo.end(), 2, "\\N");
                    }
                    else
                    {
                        hitInfo.insert(hitInfo.end(), rtData.begin(), rtData.end());
                    }
                }
                else
                {
                    hitInfo.insert(hitInfo.end(), 2, "\\N");
                }
                
            }
            // After-pulse handling -----------------
            // Check if in-time...
            if (inTime=="1")
            {
                det_el = hashData[1] + "_" + hashData[2];

                auto it = original_pulse->find(det_el);
                // Check if this element already exists in list...
                if (it == original_pulse->end())
                {
                    // Doesn't exist. Make entry
                    tdc_ind.first = atof(wordBuffer[6].data());
                    tdc_ind.second = hitStorage.size();
                    // Make an entry
                    original_pulse->insert({det_el, tdc_ind});
                }
                else
                {
                    // See if this tdcTime is greater
                    if ((it->second).first < atof(wordBuffer[6].data()))
                    {
                        // Since it is, update the map entry
                        tdc_ind.first = atof(wordBuffer[6].data());
                        tdc_ind.second = hitStorage.size();
                        (*original_pulse)[det_el] = tdc_ind;

                    }

                }
            }
            if (h1push && use_hits_for_masking)
            {
                fillTriggerGroup(hashData, inTimeHodos, BTriggerGroup, TTriggerGroup);
            }

            hitStorage.push_back(hitInfo);

            cur_tdc++;
            hitID++;
        }

        string str_cur_event = to_string(static_cast<long long>(cur_event));
        endEvent(original_pulse, inTimeHodos, inTimeRFs, hitStorage, triggerHitStorage, hitsFile,
                 triggerHitsFile, triggerRoadsFile, ithoFile, str_cur_event,
                 BTriggerGroup, TTriggerGroup, badEvents, eventBoards, runNum, spillID);
    }

    //close files
    hitsFile.close();
    triggerHitsFile.close();
    triggerRoadsFile.close();
    ithoFile.close();

    if (!nohit)
    {
        string loadHits = "LOAD DATA LOCAL INFILE '" + string(hitsFileName) + 
                          "' INTO TABLE " + schema + ".Hit";
        stmt->execute(loadHits);

        string loadTriggerHits = "LOAD DATA LOCAL INFILE '" + string(triggerHitsFileName) + 
                                 "' INTO TABLE " + schema + ".TriggerHit";
        stmt->execute(loadTriggerHits);
    }

    string loadTriggerRoads = "LOAD DATA LOCAL INFILE '" + string(triggerRoadsFileName) + 
                              "' INTO TABLE " + schema + ".TriggerRoadsTHitEvent";
    stmt->execute(loadTriggerRoads);
        
    string loadQuery = "LOAD DATA LOCAL INFILE '" + string(ithoFileName) + 
                       "' INTO TABLE " + schema + ".Occupancy";
    stmt->execute(loadQuery);

    if (badEvents.size() != 0)
    {
        stringstream ss;
        ss << "UPDATE Event SET dataQuality = dataQuality | 8 WHERE eventID in (";
        for (auto it = badEvents.begin(); it != --badEvents.end(); ++it)
        {
            ss << (*it) << ", ";
        }
        ss << badEvents.back() << ")";
        stmt->execute(ss.str());
    }

    string rmFiles = "rm " + string(hitsFileName) + " " + string(triggerHitsFileName) + 
                     " " + string(triggerRoadsFileName) + " " + string(ithoFileName);
    system(rmFiles.data());
    return 0;

}

vector< tuple< string, int, int>> Uploader::compileInTimeHodos(vector<string> &inTimeHodos)
{
    //H1s, forward list with all Hodoscope detectors with inTime = 1 for a given event
    //use HM_Map to decode
    vector< tuple< string, int, int>> maskedDs;
    for (auto it = inTimeHodos.begin(); it != inTimeHodos.end(); ++it)
    {
        //eventually make more complex
        auto tempL = (*HM_Map)[*it];
        //cout << " I'm in the compileInTimeHodos on " << (*it) << endl;
        for (auto itT = tempL.begin(); itT != tempL.end(); ++itT)
        {
            maskedDs.push_back(*itT);
            //cout << " I'm in the compileInTimeHodos in maskedDs ";
            //cout << get<0>(*itT) << " " << get<1>(*itT) << " " << get<2>(*itT) <<  endl;
        }
    }
    return maskedDs;
}

void Uploader::fillTriggerGroup(vector<string> &hashData, vector<string> &inTimeHodos,
                                vector<vector<int>> &BTriggerGroup,
                                vector<vector<int>> &TTriggerGroup)
{
    char thirdLet = hashData[1].data()[2];
    if (thirdLet == 'B')
    {
        inTimeHodos.push_back(HM_HF(hashData[1],hashData[2]));
        vector<int>* vec = &BTriggerGroup[hashData[1].data()[1]-'1'];
        int val = atoi(hashData[2].data());
        if (find(vec->begin(), vec->end(), val) == vec->end())
        {
            vec->push_back(val);
        }
    }
    else if (thirdLet == 'T')
    {
        inTimeHodos.push_back(HM_HF(hashData[1],hashData[2]));
        vector<int>* vec = &TTriggerGroup[hashData[1].data()[1]-'1'];
        int val = atoi(hashData[2].data());
        if (find(vec->begin(), vec->end(), val) == vec->end())
        {
            vec->push_back(val);
        }
    }
}

void Uploader::analyzeHits(set<string> eventRoads, vector<string> &inTimeHodos,
                           vector<vector<string>> &hitStorage, ofstream &hitsFile,
                           unordered_map< string, pair< double, int > >* original_pulse,
                           int runNum, int spillID, string &eventID, ofstream &ithoFile)
{

    int roadMaskedVal;
    int index;
    int elemID;
    int start, end;
    char fLet;
    string maskedVal;
    string DName, detName;
    string outLine;
    unsigned short original;
    unsigned short not_intime;
    unsigned short not_masking;
    unsigned short not_roadMaskedVal;
    unsigned short not_original;
    unsigned int dataQ;
    set<int> indexes;

    // Vectors for keeping track of ITHO
    vector<string> v_det {"D1", "D2", "D3", "H1", "H2", "H3", "H4", "P1", "P2"}; 
    map<string, int> det_count = {{"D1", 0}, {"D2", 0}, {"D3", 0}, {"H1", 0}, 
                                  {"H2", 0}, {"H3", 0}, {"H4", 0}, {"P1", 0},
                                  {"P2", 0}};

    for (auto it = original_pulse->begin(); it != original_pulse->end(); ++it)
    {
        // The indexes that correspond to the hitStorage vector are kept here
        indexes.insert((*it).second.second);
    }

    // Get the set of hodoscopes that occur in the roads that were reconstructed
    set<string> s_road_hodos;
    s_road_hodos = roadset->get_road_hodos(eventRoads);
  
    // Copy it to a vector<string> for use with compileInTimeHodos
    vector<string> v_road_hodos(s_road_hodos.size());
    v_road_hodos.assign(s_road_hodos.begin(), s_road_hodos.end());

    // Get the wire detectors and element ranges for these hodos
    vector<tuple< string, int, int>> roadDs = compileInTimeHodos(v_road_hodos);

    // Get the wire detectors and elements ranges for general hodoscope masking
    vector<tuple< string, int, int>> bigDs = compileInTimeHodos(inTimeHodos);
    for (auto itL = hitStorage.begin(); itL != hitStorage.end(); ++itL)
    {
        original = 0;
        roadMaskedVal = 0;
        maskedVal = "0";
        detName = (*itL)[3];
        elemID = atoi((*itL)[4].data());
        fLet = detName.data()[0];
        if ((fLet == 'D') || (fLet == 'P'))
        {
            // Hodo Masking -----------
            for (auto itD = bigDs.begin(); itD != bigDs.end(); ++itD)
            {
                DName = get<0>(*itD);
                start = get<1>(*itD);
                end = get<2>(*itD);
                if ((detName == DName) && (elemID >= start) && (elemID <= end))
                {
                    maskedVal = "1";
                    break;
                }
            }
            (*itL).push_back(maskedVal);

            // Road masking ------------
            for (auto itRD = roadDs.begin(); itRD != roadDs.end(); ++itRD)
            {
                DName = get<0>(*itRD);
                start = get<1>(*itRD);
                end = get<2>(*itRD);
                if ((detName == DName) && (elemID >= start) && (elemID <= end))
                {
                    roadMaskedVal = 1;
                    break;
                }
            }
        
        }
        else
        {
            // Non Drift Chamber or Prop Tube hits
            roadMaskedVal = 0;
            (*itL).push_back("\\N");
        }
        
        // After-Pulse Marking
        index = itL - hitStorage.begin();
        if (indexes.find(index) != indexes.end())
        {
            original = 1;
        }
       
        // If in-time, add to ITHO count
        if ((*itL)[6] == "1")
        {
            detName = ((*itL)[3]).substr(0,2);
            det_count[detName]++;
        }
       
        not_intime = !atoi((*itL)[6].data());
        not_masking = !atoi((*itL)[10].data());
        not_roadMaskedVal = !roadMaskedVal;
        not_original = !original;
        not_masking <<= 1;              // Masking is 1st bit
        not_roadMaskedVal <<= 2;        // RoadMasking is 2nd bit
        not_original <<= 3;             // OriginalPulse is 3nd bit
        dataQ = not_roadMaskedVal | not_masking | not_intime | not_original;
        //dataQuality
        (*itL).push_back(to_string(static_cast<unsigned long long>(dataQ)));
        outLine = toString(*itL);
        hitsFile.write(outLine.data(), outLine.size());
    }

    stringstream ss;
    ss << runNum << '\t' << spillID << '\t' << eventID << '\t';
    for (auto itt = v_det.begin(); itt != v_det.end(); ++itt)
    {
            ss << det_count[(*itt)] << '\t';
    }
    ss << "\n";
    outLine = ss.str();
    ithoFile.write(outLine.data(), outLine.size());

}

set<string> Uploader::analyzeTriggerRoads(string eventID, vector<vector<int>> &BTriggerGroup, 
                                   vector<vector<int>> &TTriggerGroup, 
                                   ofstream &triggerRoadsFile)
{
    set<string> eventRoads;

    //BTriggers
    auto eventB4s = BTriggerGroup[3];
    for (auto eb4 = eventB4s.begin(); eb4 != eventB4s.end(); ++eb4)
    {
        for (auto mb4 = B4->begin(); mb4 != B4->end(); ++mb4)
        {
            if ((*eb4) == get<0>(*mb4))
            {
                auto eventB3s = BTriggerGroup[2];
                for (auto eb3 = eventB3s.begin(); eb3 != eventB3s.end(); ++eb3)
                {
                    auto mapB3s = get<1>(*mb4);
                    for (auto mb3 = mapB3s->begin(); mb3 != mapB3s->end(); ++mb3)
                    {
                        if ((*eb3) == get<0>(*mb3))
                        {
                            auto eventB2s = BTriggerGroup[1];
                            for (auto eb2 = eventB2s.begin(); eb2 != eventB2s.end(); ++eb2)
                            {
                                auto mapB2s = get<1>(*mb3);
                                for (auto mb2 = mapB2s->begin(); mb2 != mapB2s->end(); ++mb2)
                                {
                                    if ((*eb2) == get<0>(*mb2))
                                    {
                                        auto eventB1s = BTriggerGroup[0];
                                        for (auto eb1 = eventB1s.begin(); eb1 != eventB1s.end(); ++eb1)
                                        {
                                            auto mapB1s = get<1>(*mb2);
                                            for (auto mb1 = mapB1s->begin(); mb1 != mapB1s->end(); ++mb1)
                                            {
                                                if ((*eb1) == get<0>(*mb1))
                                                {
                                                    eventRoads.insert(get<1>(*mb1));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    //TTriggers
    eventB4s = TTriggerGroup[3];
    for (auto eb4 = eventB4s.begin(); eb4 != eventB4s.end(); ++eb4)
    {
        for (auto mb4 = T4->begin(); mb4 != T4->end(); ++mb4)
        {
            if ((*eb4) == get<0>(*mb4))
            {
                auto eventB3s = TTriggerGroup[2];
                for (auto eb3 = eventB3s.begin(); eb3 != eventB3s.end(); ++eb3)
                {
                    auto mapB3s = get<1>(*mb4);
                    for (auto mb3 = mapB3s->begin(); mb3 != mapB3s->end(); ++mb3)
                    {
                        if ((*eb3) == get<0>(*mb3))
                        {
                            auto eventB2s = TTriggerGroup[1];
                            for (auto eb2 = eventB2s.begin(); eb2 != eventB2s.end(); ++eb2)
                            {
                                auto mapB2s = get<1>(*mb3);
                                for (auto mb2 = mapB2s->begin(); mb2 != mapB2s->end(); ++mb2)
                                {
                                    if ((*eb2) == get<0>(*mb2))
                                    {
                                        auto eventB1s = TTriggerGroup[0];
                                        for (auto eb1 = eventB1s.begin(); eb1 != eventB1s.end(); ++eb1)
                                        {
                                            auto mapB1s = get<1>(*mb2);
                                            for (auto mb1 = mapB1s->begin(); mb1 != mapB1s->end(); ++mb1)
                                            {
                                                if ((*eb1) == get<0>(*mb1))
                                                {
                                                    eventRoads.insert(get<1>(*mb1));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    for (auto road = eventRoads.begin(); road != eventRoads.end(); ++road)
    {
        string outLine = eventID + '\t' + (*road) + "\n";
        triggerRoadsFile.write(outLine.data(), outLine.size());
    }
    return eventRoads;
}

void Uploader::deleteTrigger()
{
    //T4
    for (auto m4 = T4->begin(); m4 != T4->end(); ++m4)
    {
        auto M3s = get<1>(*m4);
        for (auto m3 = M3s->begin(); m3 != M3s->end(); ++m3)
        {
            auto M2s = get<1>(*m3);
            for (auto m2 = M2s->begin(); m2 != M2s->end(); ++m2)
            {
                auto M1s = get<1>(*m2);
                delete M1s;
            }
            delete M2s;
        }
        delete M3s;
    }
    delete T4;
    //B4
    for (auto m4 = B4->begin(); m4 != B4->end(); ++m4)
    {
        auto M3s = get<1>(*m4);
        for (auto m3 = M3s->begin(); m3 != M3s->end(); ++m3)
        {
            auto M2s = get<1>(*m3);
            for (auto m2 = M2s->begin(); m2 != M2s->end(); ++m2)
            {
                auto M1s = get<1>(*m2);
                delete M1s;
            }
            delete M2s;
        }
        delete M3s;
    }
    delete B4;
}

void Uploader::endEvent(
        unordered_map< string, pair< double, int > >* original_pulse,
        vector<string> &inTimeHodos, vector<tuple<int,double>> &inTimeRFs,
        vector<vector<string>> &hitStorage, vector<vector<string>> &triggerHitStorage,
        ofstream &hitsFile, ofstream &triggerHitsFile, ofstream &triggerRoadsFile,
        ofstream &ithoFile, string &eventID,
        vector<vector<int>> &BTriggerGroup, vector<vector<int>> &TTriggerGroup,
        vector<string> &badEvents, vector<int> &eventBoards, int runNum, int spillID)
{
    
    set<string> eventRoads;
    analyzeTriggerHits(inTimeRFs, inTimeHodos, BTriggerGroup, TTriggerGroup,
                       triggerHitStorage, triggerHitsFile, eventID, badEvents,
                       eventBoards);
    eventRoads = analyzeTriggerRoads(eventID, BTriggerGroup, TTriggerGroup, triggerRoadsFile);
    analyzeHits(eventRoads, inTimeHodos, hitStorage, hitsFile, original_pulse,
                runNum, spillID, eventID, ithoFile);
    //toString reorders elements

    //clear triggerGroups
    for (int i = 0; i < 4; i++)
    {
        BTriggerGroup[i].clear();
        TTriggerGroup[i].clear();
    }

    original_pulse->clear();
    eventBoards.clear();
    hitStorage.clear();
    triggerHitStorage.clear();
    inTimeHodos.clear();
    inTimeRFs.clear();
}

void Uploader::analyzeTriggerHits(vector<tuple<int,double>> &inTimeRFs, vector<string> &inTimeHodos,
                                  vector<vector<int>> &BTriggerGroup, vector<vector<int>> &TTriggerGroup,
                                  vector<vector<string>> &triggerHitStorage, ofstream &triggerHitsFile,
                                  string &eventID, vector<string> &badEvents, vector<int> &eventBoards)
{
    sort(eventBoards.begin(), eventBoards.end());
    unordered_map<int,bool> boardMatch;

    bool (*sortPointer)(tuple<int,double>,tuple<int,double>);
    sortPointer = sortRF;
    sort(inTimeRFs.begin(), inTimeRFs.end(), sortPointer);

    //compileEventMatches
    bool addedBadEvent = false;
    for (auto i = eventBoards.begin(); i != eventBoards.end(); ++i)
    {
        tuple<int, double> searchVal = make_tuple(*i, 0.0);
        auto bound = equal_range(inTimeRFs.begin(), inTimeRFs.end(), searchVal, sortPointer);
        if ((bound.first != inTimeRFs.end()) && (bound.second-bound.first==1)) //found && unique
        {
            boardMatch[*i] = true;
        }
        else
        {
            boardMatch[*i] = false;
            if (!(addedBadEvent))
            {
                badEvents.push_back(eventID);
                addedBadEvent = true;
            }
        }
    }

    unordered_map<int,double> boardToTDC = compileInTimeRF(inTimeRFs);
    for (auto line = triggerHitStorage.begin(); line !=triggerHitStorage.end(); ++line)
    {
        int lineBoard = atoi((*line)[7].data());
        if (boardMatch[lineBoard])
        {
            string detectorName = (*line)[3];
            double tdcTime = atof((*line)[6].data());
            double RFtdcTime = boardToTDC[lineBoard];
            string inTime;
            if ((RFtdcTime <= tdcTime) && (tdcTime < RFtdcTime + 15))
            {
                inTime = "1";
            }
            else
            {
                inTime = "0";
            }
            unsigned short dataQ = !atoi(inTime.data());
            if (detectorName == "RF")
            {
                //replace boardID with inTime
                (*line)[7] = inTime;
                //errorBits
                (*line)[8] = "0";
                //dataQuality
                line->push_back(to_string(static_cast<unsigned long long>(dataQ)));
            }
            else
            {
                //inTime
                (*line)[7] = inTime;
                //errorBits
                (*line)[8] = "0";
                //dataQuality
                (*line)[9] = to_string(static_cast<unsigned long long>(dataQ));
            }
            triggerAdds((*line)[3], (*line)[4], (*line)[7], inTimeHodos,
                        BTriggerGroup, TTriggerGroup);
            string outLine = toString(*line);
            triggerHitsFile.write(outLine.data(), outLine.size());
        }
        else
        {
            string detectorName = (*line)[3];
            if (detectorName == "RF")
            {
                unsigned short dataQ = !atoi((*line)[8].data());
                //inTime
                (*line)[7] = (*line)[8];
                //errorBits
                (*line)[8] = "0";
                //dataQuality
                line->push_back(to_string(static_cast<unsigned long long>(dataQ)));
            }
            else
            {
                //hodostyle
                string inTime;
                if (((*line)[6] != "\\N") && ((*line)[8] != "\\N") && ((*line)[9] != "\\N"))
                {
                    double tdcTime = atof((*line)[6].data());
                    double tpeak = atof((*line)[8].data());
                    double width = atof((*line)[9].data());
                    if (abs(tdcTime-tpeak) <= .5 * width)
                    {
                        inTime = "1";
                    }
                    else
                    {
                        inTime = "0";
                    }
                }
                else
                {
                    inTime = "0";
                }
                unsigned short dataQ = !atoi(inTime.data());
                //inTime
                (*line)[7] = inTime;
                //errorBits
                (*line)[8] = "0";
                //dataQuality
                (*line)[9] = to_string(static_cast<unsigned long long>(dataQ));
            }
            triggerAdds((*line)[3], (*line)[4], (*line)[7], inTimeHodos,
                        BTriggerGroup, TTriggerGroup);
            string outLine = toString(*line);
            triggerHitsFile.write(outLine.data(), outLine.size());
        }
    }
}

void Uploader::triggerAdds(string detectorName, string elementID, string inTime,
                           vector<string> &inTimeHodos, vector<vector<int>> &BTriggerGroup,
                           vector<vector<int>> &TTriggerGroup)
{
    char firstLet = detectorName.data()[0];
    char thirdLet = detectorName.data()[2];
    if ((inTime == "1") && (firstLet == 'H') && ((thirdLet == 'B') || (thirdLet == 'T')))
    {
        //to work with fillTriggerGroup fix later
        vector<string> fakeHashData = {"",detectorName,elementID};
        fillTriggerGroup(fakeHashData, inTimeHodos, BTriggerGroup, TTriggerGroup);
    }
}

unordered_map<int,double> Uploader::compileInTimeRF(vector<tuple<int,double>> &inTimeRFs)
{
    unordered_map<int,double> boardToTDC;
    for (auto it = inTimeRFs.begin(); it != inTimeRFs.end(); ++it)
    {
        boardToTDC[get<0>(*it)] = get<1>(*it);
    }
    return boardToTDC;
}

string Uploader::toStrMaxDecimals(string input, int decimals)
{
    ostringstream ss;
    ss << fixed << setprecision(decimals) << atof(input.c_str());
    return ss.str();
}

Road::Road()
{
    roadID = 0;
    detectorHalf = "";
    H1 = 0;
    H2 = 0;
    H3 = 0;
    H4 = 0;
}

Road::~Road()
{
}

vector<string> Road::get_hodos()
{
    vector<string> v_hodos;
    string hodo;

    hodo = "H1" + detectorHalf + "_" + to_string(static_cast<unsigned long long>(H1));
    v_hodos.push_back(hodo);
    hodo = "H2" + detectorHalf + "_" + to_string(static_cast<unsigned long long>(H2));
    v_hodos.push_back(hodo);
    hodo = "H3" + detectorHalf + "_" + to_string(static_cast<unsigned long long>(H3));
    v_hodos.push_back(hodo);
    hodo = "H4" + detectorHalf + "_" + to_string(static_cast<unsigned long long>(H4));
    v_hodos.push_back(hodo);

    return v_hodos;
}

RoadSet::RoadSet()
{
    clear();
}

RoadSet::~RoadSet()
{
    clear();
}

void RoadSet::insert_road(int roadID, string half, int H1, int H2, int H3, int H4)
{
    Road* r = new Road();
    r->roadID = roadID;
    r->detectorHalf = half;
    r->H1 = H1;
    r->H2 = H2;
    r->H3 = H3;
    r->H4 = H4;
    v_roads.push_back(r);
}

set<string> RoadSet::get_all_hodos()
{
    set<string> s_all_hodos;
    vector<string> v_hodos;
    for (auto rd = v_roads.begin(); rd != v_roads.end(); ++rd)
    {
        v_hodos = (*rd)->get_hodos();
        for (auto hd = v_hodos.begin(); hd != v_hodos.end(); ++hd)
        {
            s_all_hodos.insert(*hd);
        }
    }
    return s_all_hodos;
}

set<string> RoadSet::get_road_hodos(set<string> eventRoads)
{
    set<int> s_eventRoads;
    set<string> s_hodos;
    vector<string> v_hodos;

    // Convert the string set of roadID's into an int set
    for (auto rd = eventRoads.begin(); rd != eventRoads.end(); ++rd)
    {
        s_eventRoads.insert(stoi(*rd));
    }

    // For each one, find the road from the roadset
    //  print out its hodos
    //  and add them to the hodo set
    for (auto rd = v_roads.begin(); rd != v_roads.end(); ++rd)
    {
        if (s_eventRoads.find((*rd)->roadID) != s_eventRoads.end())
        {
            v_hodos = (*rd)->get_hodos();
            for (auto hd = v_hodos.begin(); hd != v_hodos.end(); ++hd)
            {
                s_hodos.insert(*hd);
            }
        }
    }
    return s_hodos;
}

void RoadSet::clear()
{
    v_roads.clear();
}

