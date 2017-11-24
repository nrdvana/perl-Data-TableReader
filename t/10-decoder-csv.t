#! /usr/bin/env perl
use strict;
use warnings;
use Test::More;

use_ok( 'Data::TableReader::Decoder::CSV' ) or BAIL_OUT;

my $input= <<END;
a,b,c,d
1,2,3,4
END
open my $input_fh, '<', \$input or die;
my $d= new_ok( 'Data::TableReader::Decoder::CSV',
	[ file_name => '', file_handle => $input_fh, log => sub {} ],
	'CSV decoder' );

ok( my $iter= $d->iterator, 'got iterator' );

is_deeply( $iter->(), [ 'a', 'b', 'c', 'd' ], 'first row' );
is_deeply( $iter->(), [ '1', '2', '3', '4' ], 'second row' );
is_deeply( $iter->(), undef, 'no third row' );
ok( $iter->seek(0), 'rewind' );
is_deeply( $iter->(), [ 'a', 'b', 'c', 'd' ], 'first row again' );
is_deeply( $iter->([2,1]), [ '3', '2' ], 'slice from second row' );
ok( !$iter->next_dataset, 'no next dataset' );

done_testing;
