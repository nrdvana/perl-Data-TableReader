package Data::RecordExtractor::Decoder::CSV;

use Moo 2;
use Try::Tiny;
use Carp;
extends 'Data::RecordExtractor::Decoder';

our $_csv_class;
sub _csv_class {
	$_csv_class ||= do {
		eval { require Text::CSV_XS; }? 'Text::CSV_XS'
			: eval { require Text::CSV; }? 'Text::CSV'
			: croak "Require either Text::CSV_XS or Text::CSV : $@"
	};
}

=head1 DESCRIPTION

This decoder wraps an instance of either Text::CSV or Text::CSV_XS.
You may pass your own options via the L</parser> attribute, which
will override the defaults of this module on a per-field basis.

This module defaults to:

  parser => {
    binary => 1,
    allow_loose_quotes => 1,
    auto_diag => 2,
  }

=head1 ATTRIBUTES

=head2 parser

The instance of L<Text::CSV> or L<Text::CSV_XS>.  XS is used if it is installed.
You may pass a hashref of options to this method, or your own instance of
any module compatible with Text::CSV.

=head2 iterator

  my $iterator= $decoder->iterator;

Return an iterator which returns each row of the table as an arrayref.

=cut

has _parser_args => ( is => 'ro', init_arg => 'parser' );

has parser => ( is => 'lazy', init_arg => undef );
sub _build_parser {
	my $self= shift;
	my $args= $self->_parser_args || {};
	return $args if ref($args)->can('getline');
	return $self->_csv_class->new({ binary => 1, allow_loose_quotes => 1, auto_diag => 2, %$args });
}

sub _build_iterator {
	my $self= shift;
	my $parser= $self->parser;
	my $row= 0;
	my $fh= $self->file_handle;
	Data::RecordExtractor::Decoder::CSV::Iterator->new(
		sub {
			++$row;
			my $r= $parser->getline($fh) or return undef;
			@$r= @{$r}[ @{$_[0]} ] if $_[0]; # optional slice argument
			return $r;
		},
		{
			row => \$row,
			fh  => $fh,
			origin => tell($fh),
		}
	);
}

{ package # Hide from CPAN
	Data::RecordExtractor::Decoder::CSV::Iterator;
	use strict;
	use warnings;
	use Carp;
	use parent 'Data::RecordExtractor::Iterator';

	sub position {
		my $f= shift->_fields;
		'row '.${ $f->{row} };
	}
   
	sub progress {
		my $f= shift->_fields;
		# lazy-build the file size, using seek
		unless (exists $f->{file_size}) {
			my $pos= tell $f->{fh};
			if (defined $pos and $pos >= 0 and seek($f->{fh}, 0, 2)) {
				$f->{file_size}= tell($f->{fh});
				seek($f->{fh}, $pos, 0) or die "seek: $!";
			} else {
				$f->{file_size}= undef;
			}
		}
		return $f->{file_size}? (tell $f->{fh})/$f->{file_size} : undef;
	}

	sub tell {
		my $f= shift->_fields;
		my $pos= tell($f->{fh});
		return undef unless defined $pos && $pos >= 0;
		return [ $pos, ${$f->{row}} ];
	}

	sub seek {
		my ($self, $to)= @_;
		my $f= $self->_fields;
		defined $f->{origin} && $f->{origin} >= 0 or croak "Can't seek on source file handle";
		seek($f->{fh}, ($to? $to->[0] : $f->{origin}), 0) or croak("seek failed: $!");
		${ $f->{row} }= $to? $to->[1] : 0;
		1;
	}
}

1;
