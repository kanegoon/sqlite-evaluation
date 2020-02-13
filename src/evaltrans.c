#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include "sqlite3.h"

#define LOOP_COUNTER 100000
#define DB_NAME "eval.db"
#define SQL_BEGIN "BEGIN;"
#define SQL_COMMIT "COMMIT"
#define SQL_CREATE "CREATE TABLE evaltab (no INT, data TEXT, time REAL);"
#define SQL_WAL "PRAGMA JOURNAL_MODE=WAL;"
const char *data =
"01234567890123456789012345678901234567890123456789"
"01234567890123456789012345678901234567890123456789";

/**
 * @brief Execute starting transaction or ending transaction
 * @param *db   SQLite database handler
 * @param *cmd  SQL_BEGIN or SQL_COMMIT
 */
void do_transaction(sqlite3 *db, char *cmd){
  char *zErrMsg = 0;
  int rc = SQLITE_OK;

  rc = sqlite3_exec(db, cmd, 0, NULL, &zErrMsg);
  if(rc!=SQLITE_OK){
    fprintf(stderr, "Can not %s : %s\n", cmd, zErrMsg);
    sqlite3_free(zErrMsg);
    exit(1);
  }
}

/**
 * @brief Insert 10,000 records with transaction.
 * @param *db        SQLite database handler
 * @param num_trans  Number of transaction in one evaluation
 */
void do_evaluate(sqlite3 *db, int num_trans){
  int rc = SQLITE_OK;
  int i, j=0;
  char *zSQL = "INSERT INTO evaltab VALUES (?, ?, ?);";
  sqlite3_stmt *pStmt = 0;
  struct timespec tStart, tEnd;

  /* Compile SQL Statement */
  rc = sqlite3_prepare_v2(db, zSQL, -1, &pStmt, NULL);
  if(rc!=SQLITE_OK){
    fprintf(stderr, "Prepare Error: %s\n", sqlite3_errmsg(db));
    exit(1);
  }
  /* Get starting time */
  clock_gettime(CLOCK_REALTIME, &tStart);
  /* Loop 10,000 for Inserting records */
  for(i=0; i<LOOP_COUNTER; i++){
    if((i%num_trans)==0){
      do_transaction(db, "BEGIN");
    }
    /* Bind integer value to prepared statements */ 
    rc = sqlite3_bind_int(pStmt, 1, i);
    if(rc!=SQLITE_OK){
      fprintf(stderr, "Bind int Error: %s\n", sqlite3_errmsg(db));
      exit(1);
    }
    /* Bind text value to prepared statements */
    rc = sqlite3_bind_text(pStmt, 2, data, -1, SQLITE_TRANSIENT);
    if(rc!=SQLITE_OK){
      fprintf(stderr, "Bind text Error: %s\n", sqlite3_errmsg(db));
      exit(1);
    }
    /* Bind double value to prepared statements */
    rc = sqlite3_bind_double(pStmt, 3, (double)time(NULL));
    if(rc!=SQLITE_OK){
      fprintf(stderr, "Bind double Error: %s\n", sqlite3_errmsg(db));
      exit(1);
    }
    rc = 1;
    while(rc != SQLITE_DONE){
      /* Execute prepared statement */
      rc = sqlite3_step(pStmt);
      if (rc == SQLITE_LOCKED){
        printf("%d:%s\n",rc,sqlite3_errmsg(db));
      }
      if (!(rc == SQLITE_DONE || rc == SQLITE_BUSY || rc == SQLITE_LOCKED)){
        printf("sqlite3_step error(%d): %s\n", rc, sqlite3_errmsg(db));
        exit(1);
      }
    }
    /* Reset prepared statement */
    rc = sqlite3_reset(pStmt);
    if(((i+1)%(int)num_trans)==0 || LOOP_COUNTER<(i+1)){
      do_transaction(db, "COMMIT");
      j++;
    }
  }
  /* Get ending time */
  clock_gettime(CLOCK_REALTIME, &tEnd);
  /* Output processing time */
  printf("%7d TRANS, %7d Records : ",j,num_trans);
  if(tEnd.tv_nsec < tStart.tv_nsec){
    printf("%10ld.%09ld (sec)\n",tEnd.tv_sec - tStart.tv_sec - 1
          ,tEnd.tv_nsec + 1000000000 - tStart.tv_nsec);
  }else{
    printf("%10ld.%09ld (sec)\n",tEnd.tv_sec - tStart.tv_sec
          ,tEnd.tv_nsec - tStart.tv_nsec);
  }
  /* Destroy prepared statement */
  sqlite3_finalize(pStmt);
}

int main(int argc, char **argv){
  sqlite3 *db;
  int rc = SQLITE_OK;
  int i;
  char *zErrMsg = 0;

  printf("Transaction Evaluation, SQLite3 version = %s\n", sqlite3_version);

  for(i=LOOP_COUNTER; i>0; i=(int)i/2){
    /* Open SQLite database file */
    rc = sqlite3_open(DB_NAME, &db);
    if(rc != SQLITE_OK){
      fprintf(stderr, "Can not open database : %s\n", sqlite3_errmsg(db));
      return(1);
    }
    /* Execute creating table for evaluation */
    rc = sqlite3_exec(db, SQL_CREATE, 0, NULL, &zErrMsg);
    if(rc!=SQLITE_OK){
      fprintf(stderr, "Can not create table: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
      return(1);
    }
    do_evaluate(db, i);
    /* Close SQLite database file */
    sqlite3_close(db);
    /* Delete SQLite database file */
    remove(DB_NAME);
  }
  return(0);
}
