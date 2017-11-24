package Data::TableReader::Decoder::CSV;

use Moo 2;
use Try::Tiny;
use Carp;
extends 'Data::TableReader::Decoder';

our $_csv_class;
sub _csv_class {
	$_csv_class ||= do {
		eval { require Text::CSV_XS; }? 'Text::CSV_XS'
			: eval { require Text::CSV; }? 'Text::CSV'
			: croak "Require either Text::CSV_XS or Text::CSV : $@"
	};
}

# ABSTRACT: Access rows of a comma-delimited text file

=head1 DESCRIPTION

This decoder wraps an instance of either L<Text::CSV> or L<Text::CSV_XS>.
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

An instance of L<Text::CSV> or L<Text::CSV_XS> or compatible, or arguments to pass to the
constructor.  Constructor arguments are passed to CSV_XS if it is installed, else CSV.

=head2 iterator

  my $iterator= $decoder->iterator;

Return an L<iterator|Data::TableReader::Iterator> which returns each row of the table as an
arrayref.

=cut

has _parser_args => ( is => 'ro', init_arg => 'parser' );

has parser => ( is => 'lazy', init_arg => undef );
sub _build_parser {
	my $self= shift;
	my $args= $self->_parser_args || {};
	return $args if ref($args)->can('getline');
	return $self->_csv_class->new({ binary => 1, allow_loose_quotes => 1, auto_diag => 2, %$args });
}

has _fh_start_pos => ( is => 'rw' );
has _iterator => ( is => 'rw', weak_ref => 1 );
sub iterator {
	my $self= shift;
	croak "Multiple iterators on CSV stream not supported yet" if $self->_iterator;
	my $parser= $self->parser;
	my $row= 0;
	my $fh= $self->file_handle;
	if (defined $self->_fh_start_pos) {
		$fh->seek($self->_fh_start_pos, 0) or die "Can't seek back to start of stream";
	} else {
		$self->_fh_start_pos($fh->tell);
	}
	my $i= Data::TableReader::Decoder::CSV::_Iter->new(
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
	$self->_iterator($i);
	return $i;
}

# If you need to subclass this iterator, don't.  Just implement your own.
# i.e. I'm not declaring this implementation stable, yet.
use Data::TableReader::Iterator;
BEGIN { @Data::TableReader::Decoder::CSV::_Iter::ISA= ('Data::TableReader::Iterator'); }

sub Data::TableReader::Decoder::CSV::_Iter::position {
	my $f= shift->_fields;
	'row '.${ $f->{row} };
}

sub Data::TableReader::Decoder::CSV::_Iter::progress {
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

sub Data::TableReader::Decoder::CSV::_Iter::tell {
	my $f= shift->_fields;
	my $pos= tell($f->{fh});
	return undef unless defined $pos && $pos >= 0;
	return [ $pos, ${$f->{row}} ];
}

sub Data::TableReader::Decoder::CSV::_Iter::seek {
	my ($self, $to)= @_;
	my $f= $self->_fields;
	defined $f->{origin} && $f->{origin} >= 0 or croak "Can't seek on source file handle";
	seek($f->{fh}, ($to? $to->[0] : $f->{origin}), 0) or croak("seek failed: $!");
	${ $f->{row} }= $to? $to->[1] : 0;
	1;
}

1;
