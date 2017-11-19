package Data::RecordExtractor::Field;

use Moo 2;

# ABSTRACT: Field description for Data::RecordExtractor

=head1 DESCRIPTION

This class describes aspects of one of the fields you want to find in your spreadsheet.

=head1 ATTRIBUTES

=head2 name

Required.  Used for the hashref key if you pull records as hashes, and used in diagnostic
messages.

=head2 header

A string or regex describing the column header you want to find in the spreadsheet.
If you specify a regex, it is used directly.  If you specify a string, it becomes the regex
matching any string with the same words (\w+) and non-whitespace (\S+) characters in the same
order, case insensitive, surrounded by any amount of non-alphanumeric garbage (C<[\W_]*>).
The default (when no header is specified) is to use the L</name> as the string, but with C<"_">
replaced by whitespace.

This deserves some examples:

  Header                  Matches
  "name"                  "Name", "name", "_Name_", " ( Name )"
  "first_name"            "First_name", "First_Name", " first_name ? "
  "first name"            "first name", "First_Name", "First,\tName"
  "address:"              "ADDRESS:", "Address : ", "Address#$%^&*:/.,",
  
  Name (default header)   Matches
  "first_name"            "First Name", "first_name", "first____name"

If this default matching doesn't meet your needs or paranoia level, then you should always
specify your own header regexes.

If you set the header to C<undef>, it means the column is not identified by a header and
position must be determined by it's relation to other fields.

(If your data actually doesn't have a header and you want to assume the columns match the
fields, see extractor attribute L<Data::RecordExtractor/header_row_at>)

=head2 required

Whether or not this field must be found in order to detect a table.  Defaults to true.
Note this does U<not> mean the cell of a row must contain data in order to read a record
from the table.

=head2 trim

Boolean, default C<1>.  Whether or not to remove prefix/suffix whitespace from each value of
the field.

=head2 blank

The value to extract when the spreadsheet cell is empty.  (where "empty" depends on the value of
C<trim>).  Default is C<undef>.  Another common value would be C<"">.

=head2 type

A L<Type::Tiny> type (or any object or class with a C<check> method) or a coderef which will
validate each value pulled from a cell for this field.  This is optional and there is no
default.  The behavior of a validation failure depends on the options to RecordExtractor when
creating an iterator.

=head2 array

If true, then this field's header can be found multiple times and the value of the field will
be an arrayref of all the extracted values.  If only one column is found, the field value will
still be an arrayref (of one element).

=head2 follows

Name (or arrayref of names) of a field which this field must follow, in a first-to-last
ordering of the columns.  This field must occur immediately after the named field, or after
another field which also C<follows> that named field.

The purpose of this attribute is to resolve ambiguous columns.  Suppose you expect columns with
the following headers:

  Father    |          |      |       | Mother    |          |      |      
  FirstName | LastName | Tel. | Email | FirstName | LastName | Tel. | Email

You can use C<qr/Father\nFirstName/> to identify the first column, but after FirstName the rest
are ambiguous.  But, RecordExtractor can figure it out if you say:

  { name => 'father_first', header => qr/Father\nFirstName/ },
  { name => 'father_last',  header => 'LastName', follows => 'father_first' },
  { name => 'father_tel',   header => 'Tel.',     follows => 'father_first' },
  { name => 'father_email', header => 'Email',    follows => 'father_first' },
  ..

and so on.  Note how C<'father_first'> is used for each as the C<follows> name; this way if any
non-required fields (like maybe C<Tel>) are completely removed from the file, RecordExtractor
will still be able to find C<LastName> and C<Email>.

You can also use this to accumulate an array of columns that lack headers:

  Scores |      |       |      |       |       |       | OtherData
  12%    | 35%  | 42%   | 18%  | 65%   | 99%   | 55%   | xyz

  { name => 'scores', array => 1, trim => 1 },
  { name => 'scores', array => 1, trim => 1, header => '', follows => 'scores' },

The second field definition has an empty header, which would normally make it rather ambiguous
and potentially capture blank-header columns that might not be part of the array.  But, because
it must follow a column named 'scores' there's no ambiguity; you get exactly any column
starting from the header C<'Scores'> until a column of any other header.

=cut

has name     => ( is => 'ro', required => 1 );
has header   => ( is => 'ro' );
has required => ( is => 'ro', default => sub { 1 } );
has trim     => ( is => 'ro', default => sub { 1 } );
has blank    => ( is => 'ro' ); # default is undef
has type     => ( is => 'ro', isa => sub { $_[0]->can('check') }, required => 0 );
has array    => ( is => 'ro' );
has follows  => ( is => 'ro' );
sub follows_list { my $f= shift->follows; ref $f? @$f : defined $f? ( $f ) : () }

=head2 header_regex

L</header>, coerced to a regex if it wasn't already

=cut

has header_regex => ( is => 'lazy' );

sub _build_header_regex {
	my $self= shift;
	my $h= $self->header;
	unless (defined $h) {
		$h= $self->name;
		$h =~ s/_/ /g;
	}
	return $h if ref($h) eq 'Regexp';
	my $pattern= join "[\\W_]*", map "\Q$_\E", grep { defined && length }
		split /(\n)|\s+|(\W)/, $h; # capture newline or non-word, except for other whitespace
	return qr/^[\W_]*$pattern[\W_]*$/im;
}

1;
