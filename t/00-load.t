#!perl -T

use Test::More tests => 1;

BEGIN {
    use_ok( 'DBIx::Simple::Procedure' );
}

diag( "Testing DBIx::Simple::Procedure $DBIx::Simple::Procedure::VERSION, Perl $], $^X" );
