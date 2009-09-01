#!/usr/env/perl -w

BEGIN {
    use FindBin;
    use lib "$FindBin::Bin/lib";
}

use DBIx::Simple::Procedure;

print "\nTesting DBIx::Simple::Procedure using your local mysql server with env vars " .
      '$ENV{MYSQLDBUSER} and $ENV{MYSQLDBPASS} as your database credentials. Please set them' .
      ' and re-install if test fails.' . "\n";

# connecting to a mysql database
my $fs = "$FindBin::Bin/sql/";
my $db = DBIx::Simple::Procedure->new(
    $fs,
    'dbi:mysql:database=test', # dbi source specification
    ($ENV{MYSQLDBUSER} || 'root' ), $ENV{MYSQLDBPASS}, # username and password
);

$db->queue('tables/users/getall')->process_queue('this is a test');

 foreach my $result (@{$db->cache(0)}){
    print "$result->{info}\n";
 }
 
 print "\nDone. Found " . ( @{$db->cache(0)} || 0 ) . " records";
 