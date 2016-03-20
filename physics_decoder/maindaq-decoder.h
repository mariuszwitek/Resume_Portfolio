#ifndef __CODAREAD__
#define __CODAREAD__
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <mysql.h>
#include <string.h>
#include <libgen.h>
#include <unistd.h>
#include <regex.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <map>
#include "assert.h"

#include "TTree.h"
#include "TFile.h"

#include "docopt.c"

#include "evio.h"
#include "uploader/RunUpload.h"
#include "SRawEvent.h"

// EVIO Values
int handle;

//Decoder Values
int prod_log_id;
int log_id;
unsigned short int sampling_count;
int verbose;

// ROOT FILE HANDLING
TTree* saveTree;
TFile* saveFile;

// Run Values
unsigned short int runNum;
unsigned short int totalTriggerBits;

// Spill Values
int spillID;
unsigned char targPos;

// CODA Event Values
unsigned int eventID;
unsigned int codaEventID;

// Counters
long int hitID;

// Benchmarking Values
time_t timeStart, timeEnd;
double timeTotal;

// Values used in TDC decoding
double const trigger_pattern[8] = { 0.0, 0.0, 0.0, 0.0, 10.0, 2.5, 5.0, 7.5 };
double const channel_pattern[8] = { 10.0, 2.5, 5.0, 7.5, 0.0, 7.5, 5.0, 2.5 };
double const ts_pattern[8] = { 10.0, 2.5, 5.0, 7.5, 0.0, 0.0, 0.0, 0.0 };
unsigned int channel_bit_pattern[8] = { 0, 7, 3, 1, 15, 8, 12, 14 };
// WC TDC Values
double const wc_trigger_pattern[8] = { 0.0, 0.0, 0.0, 0.0, 40.0,
                                       10.0, 20.0, 30.0 };
double const wc_channel_pattern[8] = { 40.0, 10.0, 20.0, 30.0,
                                       0.0, 30.0, 20.0, 10.0 };
double const wc_ts_pattern[8] = { 40.0, 10.0, 20.0, 30.0, 0.0,
                                  0.0, 0.0, 0.0 };

// Some Constants used.
enum { SUCCESS = 0 };
enum { ERROR = -1 };
enum { CODA_EVENT = 0x01cc };
enum { PRESTART_EVENT = 0x001101cc };
enum { GO_EVENT = 0x001201cc };
enum { END_EVENT = 0x001401cc };
enum { PHYSICS_EVENT = 0x10cc };
enum { FEE_EVENT = 0x00840100 };
enum { FEE_PREFIX = 0x0100 };
enum { EXIT_CASE = 0x66666666 };
enum { ON = 1 };
enum { OFF = 0 };
enum { WAIT_TIME = 2 };

// Event Type Codes
enum { STANDARD_PHYSICS = 14 };
enum { IGNORE_ME = 10 };
enum { SLOW_CONTROL = 130 };
enum { RUN_DESCRIPTOR = 140 };
enum { PRESTART_INFO = 150 };
enum { BEGIN_SPILL = 11 };
enum { END_SPILL = 12 };
enum { SPILL_COUNTER = 129 };

/* ------------------------------- */
/* ------------------------------- */

// Low-level, utility functions
int file_exists (char *fileName);
int open_coda_file(char *filename);
int close_coda_file();
int read_coda_file (int physicsEvent[70000], int evnum);
int get_hex_bits (unsigned int hexNum, int numBitFromRight, int numBits);
int get_hex_bit (unsigned int hexNum, int numBitFromRight);
int get_bin_bits (unsigned int binNum, int numBitFromRight, int numBits);
int get_bin_bit (unsigned int binNum, int numBitFromRight);

// SQL Handling Functions
int createSQL (MYSQL *conn, char *db, int online);
int runExists (MYSQL *conn, int runNumber);

// CODA Event handlers
int goEventSQL (int physicsEvent[70000]);
int prestartSQL (MYSQL *conn, int physicsEvent[70000], DocoptArgs args);
int endEventSQL (int physicsEvent[70000]);
int feeEventSQL(MYSQL *conn, int physicsEvent[70000]);
int feePrescaleSQL(MYSQL *conn, int physicsEvent[70000]);

// Physics Event handler functions
int eventSQL (MYSQL *conn, int physicsEvent[70000], DocoptArgs args,
              RawEventStorage* eventStorage, RawScalerStorage* scalerStorage,
              RawHitStorage* hitStorage);
int format(MYSQL *conn, int physicsEvent[70000], int j, int v, int e906flag,
           Event* event, RawHitStorage* hitStorage, RawScalerStorage* scalerStorage);

// Special Physics Events parser functions
int eventRunDescriptorSQL(MYSQL *conn, int physicsEvent[70000]);
int eventScalerSQL (int physicsEvent[70000], int j, RawScalerStorage* scalerStorage);
int eventSlowSQL (MYSQL *conn, int physicsEvent[70000], char* dir);
int prestartInfo (MYSQL *conn, int physicsEvent[70000], char* dir);
int feeQIESQL (int physicsEvent[70000], int j, Event* event);
int eventTriggerCountSQL (int physicsEvent[70000], int j, Event* event);
int eventTriggerSQL (int physicsEvent[70000], int j, Event* e);

// Physics Event TDC Data handling functions
int eventNewTDCSQL (int physicsEvent[70000], int j, RawHitStorage* hitStorage);
int eventv1495TDCSQL (MYSQL *conn, int physicsEvent[70000], int j,
                      Event* event, RawHitStorage* hitStorage);
int eventReimerTDCSQL (int physicsEvent[70000], int j, RawHitStorage* hitStorage);
int eventWCTDCSQL (int physicsEvent[70000], int j, RawHitStorage* hitStorage);
int eventZSWCTDCSQL (int physicsEvent[70000], int j, RawHitStorage* hitStorage);
int eventJyTDCSQL (int physicsEvent[70000], int j, RawHitStorage* hitStorage);
int eventJyTDC2SQL (int physicsEvent[70000], int j, RawHitStorage* hitStorage);

// Data submitting functions
int submit_tdc_data(DocoptArgs args, RawHitStorage* hitStorage);
int submit_scaler_data(DocoptArgs args, RawScalerStorage* scalerStorage);
int submit_all_data(MYSQL *conn, DocoptArgs args, RawEventStorage* eventStorage,
                    RawHitStorage* hitStorage);

// Handlers for hitting the end of the CODA file
int retry (char *file, long int oldfilesize, int codaEventCount,
           int physicsEvent[70000]);
int wrapup(MYSQL *conn, DocoptArgs args, RawEventStorage* eventStorage,
           RawHitStorage* hitStorage);

/* ------------------------------- */
/* ------------------------------- */

int open_coda_file (char *filename)
{
    // This function calls evio's evOpen function in order to open the
    //	specified CODA file.
    //
    // Returns:	status of evOpen
    char *mode;

    mode = (char*)malloc(sizeof(char));
    sprintf(mode,"r");
    return (evOpen (filename, mode, &handle) );
}

int get_hex_bit (unsigned int hexNum, int numBitFromRight)
{
    // This function takes an integer, grabs a certain number of hexadecimal digits
    //	from a certain position in the hex representation of the number.
    //
    // For example, if number = 0x10e59c (or, 0d107356)
    //		then get_hex_bit(number, 3) would return e (or 0d14),
    //		representing 0x10e59c <-- those parts of the number
    int shift;
    unsigned int hexBit;
    // Shift the number to get rid of the bits on the right that we want
    shift = numBitFromRight;
    hexBit = hexNum;
    hexBit = hexBit >> (4 * shift);
    // Do the bitwise AND operation
    hexBit = hexBit & 0xF;
    return hexBit;
}

int get_hex_bits (unsigned int hexNum, int numBitFromRight, int numBits)
{
    // This function takes an integer, grabs a certain number of hexadecimal digits
    //	from a certain position in the hex representation of the number.
    //
    // For example, if number = 0x10e59c (or, 0d107356)
    //		then get_bin_bits(number, 3, 3) would return e59 (or 0d3673),
    //		representing 0x10e59c <-- those parts of the number
    //				 ^^^
    unsigned int hexBits = 0x0;
    int shift;
    unsigned int bitwiseand = 0xF;
    unsigned int eff = 0xF;
    int i;
    // Bitwise method.  Shift the bits, and use bitwise AND to get the bits we want
    // Shift the number to get rid of the bits on the right that we want
    shift = numBitFromRight - numBits + 1;
    hexBits = hexNum;
    hexBits = hexBits >> (4 * shift);

    // Assemble the number that we will use with the above number 
    //   in the bitwise AND operation
    //   so, if we want get_hex_bits(hexNum, 3, 2), it will make 0xFF
    for (i = 1; i < numBits; i++)
    {
        bitwiseand += (eff << (4 * i) );
    }

    // Do the bitwise AND operation
    hexBits = hexBits & bitwiseand;
    return hexBits;
}

int get_bin_bit (unsigned int binNum, int numBitFromRight)
{
    // This function takes an integer, grabs a certain binary digit
    //	from a certain position in the binary number.
    //
    // For example, if number = 11010001101011100 (or, 0d107356)
    //		then get_bin_bit(number, 3) would return 1 (or 0d01),
    //		representing 11010001101011100 <-- this part of the number
    //					  ^
    while (numBitFromRight--)
    {
        binNum /= 2;
    }

    return (binNum % 2);
}

int get_bin_bits (unsigned int binNum, int numBitFromRight, int numBits)
{
    // This function takes an integer, grabs a certain number of binary digits
    //	from a certain position in the binary number.
    //
    // For example, if number = 11010001101011100,
    //		then get_bin_bits(number, 3, 3) would return 110 (or 0d06),
    //		representing 11010001101011100 <-- those parts of the number
    //			                  ^^^
    int binBit = 0;
    int binBits = 0;
    int n = 0;
    double d = 1.0;

    for (n = 0; n < (numBits - 1); n++)
    {
        d *= 2.0;
    }

    for (n = 0; n < (numBits) && n <= numBitFromRight; n++)
    {
        binBit = get_bin_bit (binNum, numBitFromRight - n);
        binBits += binBit * d;
        d /= 2;
    }

    return binBits;
}

int read_coda_file (int physicsEvent[70000], int evnum)
{
    // Moves to the next entry in the CODA file
    // Returns:	CODA status code
    //		(See constants at top of file)
    return evRead (handle, physicsEvent, evnum);
}

int close_coda_file()
{
    // Closes the CODA file when done
    // Returns:	CODA status code
    //		(See constants at top of file)
    return evClose (handle);
}

int file_exists (char *file)
{
    // Checks to see if the file specified exists
    // Returns: 	1 if file exists,
    //  		0 if it does not
    FILE *fp;

    if ((fp = fopen (file, "r") ))
    {
        fclose (fp);
        return 1;
    }
    return 0;
}
#endif
