package Data::RecordExtractor::Decoder;

use Moo 2;

=head1 DESCRIPTION

This is an abstract base class describing the API for decoders.

A decoder's job is to iterate table rows of a file containing tabular data.
If a file provides multiple tables of data (such as worksheets, or <TABLE>
tags) then the decode should also support the "next_dataset" method.

=head1 ATTRIBUTES

=head2 filename

Set by RecordExtractor.  Useful for logging.

=head2 file_handle

Set by RecordExtractor.  This is what the iterator should parse.

=head2 logger

Set by RecordExtractor.  Unlike the attribute of the same name on
RecordExtractor, this is always a coderef, to be called as:

  $logger->($level, $message);

=head2 iterator

This lazy-builds the iterator from L</_build_iterator>

=head1 METHODS

=head2 _build_iterator

This must be implemented by the subclass, to return an instance of
L<Data::RecordExtractor::Iterator>.  The iterator should return an arrayref
each time it is called.  The iterator may also accept a single argument of
a list of columns to retrieve, rather than retrieving the full row.

=cut

has file_name   => ( is => 'ro', required => 1 );
has file_handle => ( is => 'ro', required => 1 );
has logger      => ( is => 'ro', required => 1 );
has iterator    => ( is => 'lazy' );

1;