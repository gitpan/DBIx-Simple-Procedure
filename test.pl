#!/usr/env/perl -w

BEGIN {
    use FindBin;
    use lib "$FindBin::Bin/lib";
}

use DBIx::Simple::Procedure;

# connecting to a mysql database
my $fs = "$FindBin::Bin/sql/";
my $db = DBIx::Simple::Procedure->new(
    $fs,
    'dbi:mysql:database=test', # dbi source specification
    'root', '',                     # username and password
);

$db->queue('tables/users/getall')->process_queue('this is a test');

 foreach my $result (@{$db->cache(0)}){
    print "$result->{info}\n";
 }
 
 print "\nDone. Found " . ( @{$db->cache(0)} || 0 ) . " records";
 