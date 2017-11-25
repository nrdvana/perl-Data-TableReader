#! /usr/bin/env perl
use strict;
use warnings;
use Test::More;
use Try::Tiny;
use File::Spec::Functions 'catfile';
use Data::TableReader::Decoder::XLSX;
use Data::TableReader::Decoder::XLS;

SKIP: {
skip "Need an XLS parser", 1
	unless try { Data::TableReader::Decoder::XLS->default_xls_module };
subtest XLS => sub {
	my $xls= new_ok( 'Data::TableReader::Decoder::XLS',
		[ file_name => '', file_handle => open_data('AddressAuxData.xls'), log => sub {} ],
		'XLS decoder' );
	run_test($xls);
};
}

SKIP: {
skip "Need an XLSX parser", 1
	unless try { Data::TableReader::Decoder::XLSX->default_xlsx_module };
subtest XLSX => sub {
	my $xlsx= new_ok( 'Data::TableReader::Decoder::XLSX',
		[ file_name => '', file_handle => open_data('AddressAuxData.xlsx'), log => sub {} ],
		'XLSX decoder' );
	run_test($xlsx);
};
}

done_testing;

# Both worksheets have the same data, so just repeat the tests
sub run_test {
	my $decoder= shift;

	ok( my $iter= $decoder->iterator, 'got iterator' );
	ok( my $iter2= $decoder->iterator, 'second parallel iterator' );

	is_deeply( $iter->(), [ 'Name', 'Address', 'City', 'State', 'Zip' ], 'row 1' );
	is_deeply( $iter2->(), [ 'Name', 'Address', 'City', 'State', 'Zip' ], 'row 1 from iterator 2' );
	is_deeply( $iter->(), [ 'Someone', '123 Long St', 'Somewhere', 'OH', 45678 ], 'row 2' );
	ok( my $pos= $iter->tell, 'tell position' );
	is_deeply( $iter->(), [ ('') x 5 ], 'row 3 blank' );
	is_deeply( $iter->(), [ 'Another', '01 Main St', 'Elsewhere', 'OH', 45678 ], 'row 4' );
	is_deeply( $iter->(), undef, 'no row 5' );

	ok( $iter->next_dataset, 'next dataset (worksheet)' );
	is_deeply( $iter->(), [ 'Zip Codes', '', '', '', 'Cities', '', '', '', 'State Postal Codes', '', '' ], 'sheet 2, first row' );

	ok( $iter->seek($pos), 'seek back to previous sheet' );
	is_deeply( $iter->(), [ ('') x 5 ], 'row 3 blank' );
	is_deeply( $iter->(), [ 'Another', '01 Main St', 'Elsewhere', 'OH', 45678 ], 'row 4' );
	is_deeply( $iter->(), undef, 'no row 5' );
}

sub open_data {
	my $name= shift;
	my $t_dir= __FILE__;
	$t_dir =~ s,[^\/]+$,,;
	$name= catfile($t_dir, 'data', $name);
	open(my $fh, "<:raw", $name) or die "open($name): $!";
	return $fh;
}
