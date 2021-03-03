// Copyright 2021-present 650 Industries. All rights reserved.

#import <sqlite3.h>

NS_ASSUME_NONNULL_BEGIN

@interface EXUpdatesDatabaseInitialization : NSObject

+ (BOOL)initializeDatabaseWithLatestSchemaInDirectory:(NSURL *)directory
                                             database:(struct sqlite3 **)database
                                                error:(NSError ** _Nullable)error;

/**
 * for testing purposes
 */
+ (BOOL)initializeDatabaseWithSchema:(NSString *)schema
                            filename:(NSString *)filename
                         inDirectory:(NSURL *)directory
                       shouldMigrate:(BOOL)shouldMigrate
                            database:(struct sqlite3 **)database
                               error:(NSError ** _Nullable)error;

@end

NS_ASSUME_NONNULL_END
