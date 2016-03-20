#ifndef Uploader_H
#define Uploader_H

//INCLUDES
#include <stdio.h>
#include <unordered_map>
#include <vector>
#include <set>
#include <tuple>
#include <iostream>
#include <fstream>
#include <sstream>
#include <stdlib.h> //atoi
#include <cmath>
#include <algorithm> //find
#include <iostream>
#include <cstring>
#include <string>

//mysql include
#include <cppconn/driver.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/exception.h>

#include "../SRawEvent.h"
#include "../RawStorage.h"

//timing
#include <time.h>

using namespace std;
using namespace sql;

void InitMap();
SRawEvent* rawEvent;
std::map<int, SRawEvent> rawEventBuffer;

class Road
{
public:
    Road();
    ~Road();

    int roadID;
    string detectorHalf;
    int H1;
    int H2;
    int H3;
    int H4;

    vector<string> get_hodos();

};

class RoadSet
{
public:
    RoadSet();
    ~RoadSet();
    
    vector<Road*> v_roads;

    void insert_road(int roadID, string half, int H1, int H2, int H3, int H4);
    set<string> get_all_hodos();   
    set<string> get_road_hodos(set<string> eventRoads);   
    void clear();
};

class Uploader
{
public:
    Uploader();
    ~Uploader();

    //app_name app_number
    //===================
    //HODO_INFO	    0
    //CHAMBER_INFO  1
    //RT		    2
    //HODOMASK	    3
    //TRIGGER_ROADS	4
    //TRIGGER_INFO	5
    //SCALER_INFO	6

    //init Uploader with calibration information from result set and corresponding app number
    int initialize(ResultSet *res, int app);

    int decodeScaler(string path, string server, string schema, Statement *stmt,
                     long int hitID, RawScalerStorage* scalerStorage);
    int decodeTDC(string path, string server, string schema, Statement *stmt,
                  long int hitID, int runNum, int spillID, RawHitStorage* rawStorage,
                  int nohit);

    //round the float number up according to the given decimals
    string toStrMaxDecimals(string str, int decimals);

private:
    //const char DELIMITER = '\t';
    //const int USE_HITS_FOR_MASKING_BEFORE_THIS_RUN = 11795;

    //temp datastructures and methods for calibration information
    //====================================

    //HODOINFO and CHAMBERINFO
    unordered_map<unsigned long, vector<string>>* HIC_Map;
    unsigned long HIC_HF(string &a, string &b, string &c);

    //RT
    unordered_map<string, vector<string>>* RT_Map;
    string RT_HF(string &a, string &b);

    //HODOMASK
    unordered_map< string, vector< tuple< string, int, int>>>* HM_Map;
    string HM_HF(string a, string b);
    vector< tuple< string, int, int>> compileInTimeHodos(vector<string> &inTimeHodos);

    //TRIGGER_ROAD
    vector< tuple< int, vector< tuple< int, vector< tuple< int, vector< tuple<int, string>>*> >*> >*> >* T4;
    vector< tuple< int, vector< tuple< int, vector< tuple< int, vector< tuple<int, string>>*> >*> >*> >* B4;

    //TRIGGERINFO
    unordered_map<unsigned long, vector<string>>* TI_Map;
    unsigned long TI_HF(string &a, string &b, string &c);
    unordered_map<int,double> compileInTimeRF(vector<tuple<int,double>> &inTimeRFs);

    //SCALERINFO
    unordered_map<unsigned long, string>* SI_Map;
    unsigned long SI_HF(string &a, string &b, string &c);

    //ROAD_MASKING
    RoadSet* roadset;
    int use_hits_for_masking;

    //AFTER_PULSE
    unordered_map< string, pair< double, int > >* original_pulse; 
    //======================================

    string toString(vector<string> &input);
    void fillTriggerGroup(vector<string> &hashData, vector<string> &inTimeHodos, 
                          vector<vector<int>> &BTriggerGroup, 
                          vector<vector<int>> &TTriggerGroup);
    void analyzeHits(set<string> eventRoads, vector<string> &inTimeHodos,
                     vector<vector<string>> &hitStorage, ofstream &hitsFile,
                     unordered_map< string, pair< double, int > >* original_pulse,
                     int runNum, int spillID, string &eventID, ofstream &ithoFile);
    void analyzeTriggerHits(vector<tuple<int,double>> &inTimeRFs, 
                            vector<string> &inTimeHodos,
                            vector<vector<int>> &BTriggerGroup,
                            vector<vector<int>> &TTriggerGroup, 
                            vector<vector<string>> &triggerHitStroage,
                            ofstream &triggerHitsFile, string &prevEvent, 
                            vector<string> &badEvents, vector<int> &eventBoards);
    void triggerAdds(string detectorName, string elementID, string inTime,
                     vector<string> &inTimeHodos, vector<vector<int>> &BTriggerGroup,
                     vector<vector<int>> &TTriggerGroup);
    set<string> analyzeTriggerRoads(
            string prevEvent, vector<vector<int>> &BTriggerGroup,
            vector<vector<int>> &TTriggerGroup, ofstream &triggerRoadsFile);
    void deleteTrigger();
    void endEvent(unordered_map< string, pair< double, int > >* original_pulse,
                  vector<string> &inTimeHodos, vector<tuple<int,double>> &inTimeRFs,
                  vector<vector<string>> &hitStorage,
                  vector<vector<string>> &triggerHitStorage,
                  ofstream &hitsFile, ofstream &triggerHitsFile,
                  ofstream &triggerRoadsFile, ofstream &ithoFile, string &eventID,
                  vector<vector<int>> &BTriggerGroup,
                  vector<vector<int>> &TTriggerGroup,
                  vector<string> &badEvents, vector<int> &eventBoards,
                  int runNum, int spillID);
    //bool sortRF(tuple<int,double> a, tuple<int,double> b);
};

std::map<std::string, int> map_detectorID;
int getDetectorID(std::string detectorName)
{
    return map_detectorID[detectorName];
}

void InitMap()
{

    //Initialize the detectorID --- detectorName convention
    typedef std::map<std::string, int>::value_type nameToID;

    map_detectorID.insert(nameToID("D1U", 1));
    map_detectorID.insert(nameToID("D1Up", 2));
    map_detectorID.insert(nameToID("D1X", 3));
    map_detectorID.insert(nameToID("D1Xp", 4));
    map_detectorID.insert(nameToID("D1V", 5));
    map_detectorID.insert(nameToID("D1Vp", 6));
    map_detectorID.insert(nameToID("D2V", 7));
    map_detectorID.insert(nameToID("D2Vp", 8));
    map_detectorID.insert(nameToID("D2Xp", 9));
    map_detectorID.insert(nameToID("D2X", 10));
    map_detectorID.insert(nameToID("D2U", 11));
    map_detectorID.insert(nameToID("D2Up", 12));
    map_detectorID.insert(nameToID("D3pVp", 13));
    map_detectorID.insert(nameToID("D3pV", 14));
    map_detectorID.insert(nameToID("D3pXp", 15));
    map_detectorID.insert(nameToID("D3pX", 16));
    map_detectorID.insert(nameToID("D3pUp", 17));
    map_detectorID.insert(nameToID("D3pU", 18));
    map_detectorID.insert(nameToID("D3mVp", 19));
    map_detectorID.insert(nameToID("D3mV", 20));
    map_detectorID.insert(nameToID("D3mXp", 21));
    map_detectorID.insert(nameToID("D3mX", 22));
    map_detectorID.insert(nameToID("D3mUp", 23));
    map_detectorID.insert(nameToID("D3mU", 24));
    map_detectorID.insert(nameToID("H1B", 25));
    map_detectorID.insert(nameToID("H1T", 26));
    map_detectorID.insert(nameToID("H1L", 27));
    map_detectorID.insert(nameToID("H1R", 28));
    map_detectorID.insert(nameToID("H2L", 29));
    map_detectorID.insert(nameToID("H2R", 30));
    map_detectorID.insert(nameToID("H2B", 31));
    map_detectorID.insert(nameToID("H2T", 32));
    map_detectorID.insert(nameToID("H3B", 33));
    map_detectorID.insert(nameToID("H3T", 34));
    map_detectorID.insert(nameToID("H4Y1L", 35));
    map_detectorID.insert(nameToID("H4Y1R", 36));
    map_detectorID.insert(nameToID("H4Y2L", 37));
    map_detectorID.insert(nameToID("H4Y2R", 38));
    map_detectorID.insert(nameToID("H4B", 39));
    map_detectorID.insert(nameToID("H4T", 40));
    map_detectorID.insert(nameToID("P1Hf", 41));
    map_detectorID.insert(nameToID("P1Hb", 42));
    map_detectorID.insert(nameToID("P1Vf", 43));
    map_detectorID.insert(nameToID("P1Vb", 44));
    map_detectorID.insert(nameToID("P2Vf", 45));
    map_detectorID.insert(nameToID("P2Vb", 46));
    map_detectorID.insert(nameToID("P2Hf", 47));
    map_detectorID.insert(nameToID("P2Hb", 48));

    map_detectorID.insert(nameToID("H4Bu", 39));
    map_detectorID.insert(nameToID("H4Bd", 39));
    map_detectorID.insert(nameToID("H4Tu", 40));
    map_detectorID.insert(nameToID("H4Td", 40));
    map_detectorID.insert(nameToID("H4Y1Ll", 35));
    map_detectorID.insert(nameToID("H4Y1Lr", 35));
    map_detectorID.insert(nameToID("H4Y1Rl", 36));
    map_detectorID.insert(nameToID("H4Y1Rr", 36));
    map_detectorID.insert(nameToID("H4Y2Ll", 37));
    map_detectorID.insert(nameToID("H4Y2Lr", 37));
    map_detectorID.insert(nameToID("H4Y2Rl", 38));
    map_detectorID.insert(nameToID("H4Y2Rr", 38));

    for (int iPropPlane=1; iPropPlane<=9; iPropPlane++)
    {
        Char_t tmpName[8][10];
        sprintf(tmpName[0],"P1H%df",iPropPlane);
        sprintf(tmpName[1],"P1H%db",iPropPlane);
        sprintf(tmpName[2],"P1V%df",iPropPlane);
        sprintf(tmpName[3],"P1V%db",iPropPlane);
        sprintf(tmpName[4],"P2V%df",iPropPlane);
        sprintf(tmpName[5],"P2V%db",iPropPlane);
        sprintf(tmpName[6],"P2H%df",iPropPlane);
        sprintf(tmpName[7],"P2H%db",iPropPlane);

        map_detectorID.insert(nameToID(tmpName[0], 41));
        map_detectorID.insert(nameToID(tmpName[1], 42));
        map_detectorID.insert(nameToID(tmpName[2], 43));
        map_detectorID.insert(nameToID(tmpName[3], 44));
        map_detectorID.insert(nameToID(tmpName[4], 45));
        map_detectorID.insert(nameToID(tmpName[5], 46));
        map_detectorID.insert(nameToID(tmpName[6], 47));
        map_detectorID.insert(nameToID(tmpName[7], 48));
    }

    // not needed in SRawEvent
    // map_detectorID.insert(nameToID("L1",49));
    // map_detectorID.insert(nameToID("BeforeInhNIM",50));
    // map_detectorID.insert(nameToID("BeforeInhMatrix",51));
    // map_detectorID.insert(nameToID("AfterInhNIM",52));
    // map_detectorID.insert(nameToID("AfterInhMatrix",53));
    // map_detectorID.insert(nameToID("BOS",54));
    // map_detectorID.insert(nameToID("EOS",55));
    // map_detectorID.insert(nameToID("RF",56));
}

#endif
