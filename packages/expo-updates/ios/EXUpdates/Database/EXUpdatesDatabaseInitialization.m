// Copyright 2021-present 650 Industries. All rights reserved.

#import <EXUpdates/EXUpdatesDatabaseInitialization.h>
#import <EXUpdates/EXUpdatesDatabaseUtils.h>

NS_ASSUME_NONNULL_BEGIN

static NSString * const EXUpdatesDatabaseInitializationErrorDomain = @"EXUpdatesDatabaseInitialization";
static NSString * const EXUpdatesDatabaseV4Filename = @"expo-v4.db";
static NSString * const EXUpdatesDatabaseLatestFilename = @"expo-v5.db";

static NSString * const EXUpdatesDatabaseInitializationLatestSchema = @"\
CREATE TABLE \"updates\" (\
\"id\"  BLOB UNIQUE,\
\"scope_key\"  TEXT NOT NULL,\
\"commit_time\"  INTEGER NOT NULL,\
\"runtime_version\"  TEXT NOT NULL,\
\"launch_asset_id\" INTEGER,\
\"metadata\"  TEXT,\
\"status\"  INTEGER NOT NULL,\
\"keep\"  INTEGER NOT NULL,\
PRIMARY KEY(\"id\"),\
FOREIGN KEY(\"launch_asset_id\") REFERENCES \"assets\"(\"id\") ON DELETE CASCADE\
);\
CREATE TABLE \"assets\" (\
\"id\"  INTEGER PRIMARY KEY AUTOINCREMENT,\
\"url\"  TEXT,\
\"key\"  TEXT UNIQUE,\
\"headers\"  TEXT,\
\"type\"  TEXT NOT NULL,\
\"metadata\"  TEXT,\
\"download_time\"  INTEGER NOT NULL,\
\"relative_path\"  TEXT NOT NULL,\
\"hash\"  BLOB NOT NULL,\
\"hash_type\"  INTEGER NOT NULL,\
\"marked_for_deletion\"  INTEGER NOT NULL\
);\
CREATE TABLE \"updates_assets\" (\
\"update_id\"  BLOB NOT NULL,\
\"asset_id\" INTEGER NOT NULL,\
FOREIGN KEY(\"update_id\") REFERENCES \"updates\"(\"id\") ON DELETE CASCADE,\
FOREIGN KEY(\"asset_id\") REFERENCES \"assets\"(\"id\") ON DELETE CASCADE\
);\
CREATE TABLE \"json_data\" (\
\"id\" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,\
\"key\" TEXT NOT NULL,\
\"value\" TEXT NOT NULL,\
\"last_updated\" INTEGER NOT NULL,\
\"scope_key\" TEXT NOT NULL\
);\
CREATE UNIQUE INDEX \"index_updates_scope_key_commit_time\" ON \"updates\" (\"scope_key\", \"commit_time\");\
CREATE INDEX \"index_updates_launch_asset_id\" ON \"updates\" (\"launch_asset_id\");\
CREATE INDEX \"index_json_data_scope_key\" ON \"json_data\" (\"scope_key\")\
";

@implementation EXUpdatesDatabaseInitialization

+ (BOOL)initializeDatabaseWithLatestSchemaInDirectory:(NSURL *)directory
                                             database:(struct sqlite3 **)database
                                                error:(NSError ** _Nullable)error
{
  return [[self class] initializeDatabaseWithSchema:EXUpdatesDatabaseInitializationLatestSchema
                                        inDirectory:directory
                                           database:database
                                              error:error];
}

+ (BOOL)initializeDatabaseWithSchema:(NSString *)schema
                         inDirectory:(NSURL *)directory
                            database:(struct sqlite3 **)database
                               error:(NSError ** _Nullable)error
{
  sqlite3 *db;
  NSURL *dbUrl = [directory URLByAppendingPathComponent:EXUpdatesDatabaseLatestFilename];
  BOOL shouldInitializeDatabaseSchema = ![[NSFileManager defaultManager] fileExistsAtPath:[dbUrl path]];

  BOOL didMigrate = [[self class] _migrateDatabaseInDirectory:directory];
  if (!didMigrate) {
    NSError *removeFailedMigrationError;
    if ([NSFileManager.defaultManager fileExistsAtPath:dbUrl.path] &&
        ![NSFileManager.defaultManager removeItemAtPath:dbUrl.path error:&removeFailedMigrationError]) {
      if (error != nil) {
        NSString *description = [NSString stringWithFormat:@"Failed to migrate database, then failed to remove old database file: %@", removeFailedMigrationError.localizedDescription];
        *error = [NSError errorWithDomain:EXUpdatesDatabaseInitializationErrorDomain
                                     code:1022
                                 userInfo:@{ NSLocalizedDescriptionKey: description, NSUnderlyingErrorKey: removeFailedMigrationError }];
      }
      return NO;
    }
    shouldInitializeDatabaseSchema = YES;
  } else {
    shouldInitializeDatabaseSchema = NO;
  }

  int resultCode = sqlite3_open([[dbUrl path] UTF8String], &db);
  if (resultCode != SQLITE_OK) {
    NSLog(@"Error opening SQLite db: %@", [EXUpdatesDatabaseUtils errorFromSqlite:db].localizedDescription);
    sqlite3_close(db);

    if (resultCode == SQLITE_CORRUPT || resultCode == SQLITE_NOTADB) {
      NSString *archivedDbFilename = [NSString stringWithFormat:@"%f-%@", [[NSDate date] timeIntervalSince1970], EXUpdatesDatabaseLatestFilename];
      NSURL *destinationUrl = [directory URLByAppendingPathComponent:archivedDbFilename];
      NSError *err;
      if ([[NSFileManager defaultManager] moveItemAtURL:dbUrl toURL:destinationUrl error:&err]) {
        NSLog(@"Moved corrupt SQLite db to %@", archivedDbFilename);
        if (sqlite3_open([[dbUrl absoluteString] UTF8String], &db) != SQLITE_OK) {
          if (error != nil) {
            *error = [EXUpdatesDatabaseUtils errorFromSqlite:db];
          }
          return NO;
        }
        shouldInitializeDatabaseSchema = YES;
      } else {
        NSString *description = [NSString stringWithFormat:@"Could not move existing corrupt database: %@", [err localizedDescription]];
        if (error != nil) {
          *error = [NSError errorWithDomain:EXUpdatesDatabaseInitializationErrorDomain
                                       code:1004
                                   userInfo:@{ NSLocalizedDescriptionKey: description, NSUnderlyingErrorKey: err }];
        }
        return NO;
      }
    } else {
      if (error != nil) {
        *error = [EXUpdatesDatabaseUtils errorFromSqlite:db];
      }
      return NO;
    }
  }

  // foreign keys must be turned on explicitly for each database connection
  NSError *pragmaForeignKeysError;
  if (![EXUpdatesDatabaseUtils executeSql:@"PRAGMA foreign_keys=ON;" withArgs:nil onDatabase:db error:&pragmaForeignKeysError]) {
    NSLog(@"Error turning on foreign key constraint: %@", pragmaForeignKeysError.localizedDescription);
  }

  if (shouldInitializeDatabaseSchema) {
    char *errMsg;
    if (sqlite3_exec(db, schema.UTF8String, NULL, NULL, &errMsg) != SQLITE_OK) {
      if (error != nil) {
        *error = [EXUpdatesDatabaseUtils errorFromSqlite:db];
      }
      sqlite3_free(errMsg);
      return NO;
    };
  }

  *database = db;
  return YES;
}

+ (BOOL)_migrateDatabaseInDirectory:(NSURL *)directory
{
  NSURL *latestURL = [directory URLByAppendingPathComponent:EXUpdatesDatabaseLatestFilename];
  NSURL *v4URL = [directory URLByAppendingPathComponent:EXUpdatesDatabaseV4Filename];
  if ([NSFileManager.defaultManager fileExistsAtPath:latestURL.path]) {
    return NO;
  }
  if ([NSFileManager.defaultManager fileExistsAtPath:v4URL.path]) {
    NSError *fileMoveError;
    if (![NSFileManager.defaultManager moveItemAtPath:v4URL.path toPath:latestURL.path error:&fileMoveError]) {
      NSLog(@"Migration failed: failed to rename database file");
      return NO;
    }
    sqlite3 *db;
    if (sqlite3_open(latestURL.absoluteString.UTF8String, &db) != SQLITE_OK) {
      NSLog(@"Error opening migrated SQLite db: %@", [EXUpdatesDatabaseUtils errorFromSqlite:db].localizedDescription);
      sqlite3_close(db);
      return NO;
    }

    NSError *migrationError;
    if (![self _migrate4To5:db error:&migrationError]) {
      NSLog(@"Error migrating SQLite db from v4 to v5: %@", [EXUpdatesDatabaseUtils errorFromSqlite:db].localizedDescription);
      sqlite3_close(db);
      return NO;
    }

    // migration was successful
    sqlite3_close(db);
    return YES;
  }
}

+ (BOOL)_migrate4To5:(sqlite3 *)db error:(NSError **)error
{
  // https://www.sqlite.org/lang_altertable.html#otheralter
  if (sqlite3_exec(db, "PRAGMA foreign_keys=OFF;", NULL, NULL, NULL) != SQLITE_OK) return NO;
  if (sqlite3_exec(db, "BEGIN;", NULL, NULL, NULL) != SQLITE_OK) return NO;

  NSString * const migrationSQL = @"CREATE TABLE \"assets\" (\
  \"id\"  INTEGER PRIMARY KEY AUTOINCREMENT,\
  \"url\"  TEXT,\
  \"key\"  TEXT UNIQUE,\
  \"headers\"  TEXT,\
  \"type\"  TEXT NOT NULL,\
  \"metadata\"  TEXT,\
  \"download_time\"  INTEGER NOT NULL,\
  \"relative_path\"  TEXT NOT NULL,\
  \"hash\"  BLOB NOT NULL,\
  \"hash_type\"  INTEGER NOT NULL,\
  \"marked_for_deletion\"  INTEGER NOT NULL\
  );\
  INSERT INTO `new_assets` (`id`, `url`, `key`, `headers`, `type`, `metadata`, `download_time`, `relative_path`, `hash`, `hash_type`, `marked_for_deletion`)\
  SELECT `id`, `url`, `key`, `headers`, `type`, `metadata`, `download_time`, `relative_path`, `hash`, `hash_type`, `marked_for_deletion` FROM `assets`;\
  DROP TABLE `assets`;\
  ALTER TABLE `new_assets` RENAME TO `assets`;";
  if (sqlite3_exec(db, migrationSQL.UTF8String, NULL, NULL, NULL) != SQLITE_OK) {
    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    return NO;
  }

  if (sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK) return NO;
  return YES;
}

@end

NS_ASSUME_NONNULL_END
