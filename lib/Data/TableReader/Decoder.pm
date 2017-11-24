package Data::TableReader::Decoder;

use Moo 2;

# ABSTRACT: Base class for table decoders

=head1 DESCRIPTION

This is an abstract base class describing the API for decoders.

A decoder's job is to iterate table rows of a file containing tabular data.
If a file provides multiple tables of data (such as worksheets, or <TABLE>
tags) then the decode should also support the "next_dataset" method.

=head1 ATTRIBUTES

=head2 filename

Set by TableReader.  Useful for logging.

=head2 file_handle

Set by TableReader.  This is what the iterator should parse.

=head2 log

Set by TableReader.  Unlike the attribute of the same name on
TableReader, this is always a coderef, to be called as:

  $log->($level, $message);

=head1 METHODS

=head2 iterator

This must be implemented by the subclass, to return an instance of
L<Data::TableReader::Iterator>.  The iterator should return an arrayref each time it is called,
and accept one optional argument of a "slice" needed from the record.
All decoder iterators return arrayrefs, so the slice should be an arrayref of column indicies
equivalent to the perl syntax

  @row[@$slice]

=cut

has file_name   => ( is => 'ro', required => 1 );
has file_handle => ( is => 'ro', required => 1 );
has log         => ( is => 'ro', required => 1 );

1;
