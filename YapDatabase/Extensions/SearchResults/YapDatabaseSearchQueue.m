#import "YapDatabaseSearchQueue.h"
#import "YapDatabaseSearchQueuePrivate.h"

#import <libkern/OSAtomic.h>
#import <os/lock.h>

@interface YapDatabaseSearchQueueControl : NSObject

- (id)initWithRollback:(BOOL)rollback;

@property (nonatomic, readonly) BOOL abort;
@property (nonatomic, readonly) BOOL rollback;

@end

@implementation YapDatabaseSearchQueueControl

@synthesize rollback = rollback;

- (id)initWithRollback:(BOOL)inRollback
{
	if ((self = [super init]))
	{
		rollback = inRollback;
	}
	return self;
}

- (BOOL)abort {
	return YES;
}

@end

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark -
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

@implementation YapDatabaseSearchQueue
{
	NSMutableArray *queue;
	os_unfair_lock lock;
	
	BOOL queueHasAbort;
	BOOL queueHasRollback;
}

- (id)init
{
	if ((self = [super init]))
	{
		queue = [[NSMutableArray alloc] init];
		lock = OS_UNFAIR_LOCK_INIT;
	}
	return self;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Public API
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (void)enqueueQuery:(NSString *)query
{
	if (query == nil) return;
	
	os_unfair_lock_lock(&lock);
	{
		[queue addObject:[query copy]];
	}
	os_unfair_lock_unlock(&lock);
}

- (void)abortSearchInProgressAndRollback:(BOOL)shouldRollback
{
	os_unfair_lock_lock(&lock);
	{
		YapDatabaseSearchQueueControl *control =
		  [[YapDatabaseSearchQueueControl alloc] initWithRollback:shouldRollback];
		
		[queue addObject:control];
		
		queueHasAbort = YES;
		queueHasRollback = queueHasRollback || shouldRollback;
	}
	os_unfair_lock_unlock(&lock);
}

- (NSArray *)enqueuedQueries
{
	NSMutableArray *queries = nil;
	
	os_unfair_lock_lock(&lock);
	{
		queries = [NSMutableArray arrayWithCapacity:[queue count]];
		
		for (id obj in queue)
		{
			if ([obj isKindOfClass:[NSString class]])
			{
				[queries addObject:obj];
			}
		}
	}
	os_unfair_lock_unlock(&lock);
	
	return queries;
}

- (NSUInteger)enqueuedQueryCount
{
	NSUInteger count = 0;
	
	os_unfair_lock_lock(&lock);
	{
		for (id obj in queue)
		{
			if ([obj isKindOfClass:[NSString class]])
			{
				count++;
			}
		}
	}
	os_unfair_lock_unlock(&lock);
	
	return count;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma mark Private API
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

- (NSString *)flushQueue
{
	NSString *lastQuery = nil;
	
	os_unfair_lock_lock(&lock);
	{
		id lastObject = [queue lastObject];
		[queue removeAllObjects];
		
		queueHasAbort = NO;
		queueHasRollback = NO;
		
		if ([lastObject isKindOfClass:[NSString class]])
		{
			lastQuery = (NSString *)lastObject;
		}
	}
	os_unfair_lock_unlock(&lock);
	
	return lastQuery;
}

- (BOOL)shouldAbortSearchInProgressAndRollback:(BOOL *)shouldRollbackPtr
{
	BOOL shouldAbort = NO;
	BOOL shouldRollback = NO;
	
	os_unfair_lock_lock(&lock);
	{
		shouldAbort = queueHasAbort;
		shouldRollback = queueHasRollback;
	}
	os_unfair_lock_unlock(&lock);
	
	if (shouldRollbackPtr) *shouldRollbackPtr = shouldRollback;
	return shouldAbort;
}

@end
