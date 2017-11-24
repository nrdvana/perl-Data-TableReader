#! /usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Log::Any '$log';
use Log::Any::Adapter 'TAP';

use_ok( 'Data::TableReader' ) or BAIL_OUT;

subtest iterator_weakref => sub {
	open(my $csv, '<', \"a,b,c\n1,2,3\n") or die;
	my $re= new_ok( 'Data::TableReader',
		[ input => $csv, decoder => 'CSV', fields => ['a','b','c'], log => $log ],
		'TableReader'
	);
	ok( $re->find_table, 'find_table' ) or die "Can't continue without table";
	ok( my $i= $re->iterator, 'create iterator' );
	my $i_name= "$i";
	undef $i;
	is( $re->_iterator, undef, 'iterator was garbage collected' );
	ok( my $i2= $re->iterator, 'second interator' );
	is_deeply( $i2->all, [ { a => 1, b => 2, c => 3 } ], 'read rows' );
};

done_testing;
