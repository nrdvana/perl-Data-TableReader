#! /usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Data::TableReader::Decoder::IdiotCSV;

plan skip_all => 'Need a CSV parser for this test'
	unless Data::TableReader::Decoder::IdiotCSV->default_csv_module;

my $input= <<END;
"First Name","Last Name","Nickname"
"Joe","Smith",""Bossman, and a half!""
END
open my $input_fh, '<', \$input or die;
my $d= new_ok( 'Data::TableReader::Decoder::IdiotCSV',
	[ file_name => '', file_handle => $input_fh, log => sub {} ],
	'IdiotCSV decoder' );

ok( my $iter= $d->iterator, 'got iterator' );

is_deeply( $iter->(), [ 'First Name', 'Last Name', 'Nickname' ], 'first row' );
is_deeply( $iter->(), [ 'Joe', 'Smith', '"Bossman, and a half!"' ], 'second row' );
is_deeply( $iter->(), undef, 'no third row' );

done_testing;
