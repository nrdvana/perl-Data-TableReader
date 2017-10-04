package Data::RecordExtractor::Iterator;

use strict;
use warnings;
use Try::Tiny;
use Carp;
use Scalar::Util 'refaddr';

=head1 SYNOPSIS

  my $iter= $record_extractor->iterator;
  while (my $rec= $iter->()) {
    ...
    my $position= $iter->tell;
    print "Marking position $position"; # position stringifies to human-readable
    ...
    $iter->seek($position);
  }
  if ($iter->next_dataset) {
    # iterate some more
    while ($rec= $iter->()) {
      ...
      printf "Have processed %3d %% of the file", $iter->progress*100;
    }
  }

=head1 DESCRIPTION

This is the abstract base class for iterators used in Data::RecordExtractor,
which are blessed coderefs that return records on each call.

=head1 ATTRIBUTES

=head2 position

Return a human-readable string describing the current location within the source file.
This will be something like C<"$filename row $row"> or C<"$filename $worksheet:$cell_id">.

=head2 progress

An estimate of how much of the data has already been returned.  If the stream
is not seekable this may return undef.

=head1 METHODS

=head2 tell

If seeking is supported, this will return some value that can be passed to
seek to come back to this point in the stream.  This value will always be
true. If seeking is not supported this will return undef.

=head2 seek

  $iter->seek($pos);

Seek to a point previously reported by L</tell>.  If seeking is not supported
this will die.  If C<$pos> is any false value it means to seek to the start of
the stream.

=head2 next_dataset

If a file format supports more than one tabular group of data, this method
allows you to jump to the next.

=cut

our %_iterator_fields;
sub new {
	my ($class, $sub, $fields)= @_;
	ref $sub eq 'CODE' and ref $fields eq 'HASH'
		or die "Expected new(CODEREF, HASHREF)";
	$_iterator_fields{refaddr $sub}= $fields;
	return bless $sub, $class;
}

sub _fields {
	$_iterator_fields{refaddr shift};
}

sub DESTROY {
	delete $_iterator_fields{refaddr shift};
}

sub next_dataset {
	undef;
}

1;
