package Data::RecordExtractor;

use Moo 2;
use Try::Tiny;
use URI;
use Carp;
use Log::Any '$log';
use Module::Runtime 'require_module';
use Data::RecordExtractor::Field;

# ABSTRACT: Extract records from "dirty" tabular data sources

=head1 SYNOPSIS

  my $ex= Data::RecordExtractor->new(
    # path or file handle
    input => 'path/to/file.csv',
    # let it auto-detect the format (but can override that if we need)
    # We want these fields to exist in the file (identified by headers)
    fields => [
      { name => 'address', header => qw/street|address/i },
      'city',
      'state',
      # can validate with Type::Tiny classes
      { name => 'zip', header => qw/zip\b|postal/i, type => US_Zipcode },
    ],
    # could do this after extraction, but this fixes it before the type validation
    filters => [
       # they keep losing the leading zeroes on our zip codes. grr.
       sub {
         $_[0]{zip} =~ s/^(\d+)(-(\d+))?$/sprintf("%05d-%04d", $1, $3||0)/e;
         return $_[0];
       },
    ],
    # Our data provider is horrible, just ignore any nonsense we encounter
    on_blank_rows => 'next',
    on_validation_fail => 'next',
    # Capture warnings and show to user who uploaded file
    logger => \(my @messages)
  );
  
  my $records= $ex->iterator->all;
  ...
  $http_response->body( encode_json({ messages => \@messages }) );

=head1 DESCRIPTION

This module is designed to be a useful for anyone who needs to take "loose"
or "dirty" tabular data sources (such as Excel, CSV, TSV, or HTML) which may
have been edited by non-technical humans and extract the data into sanitized
records, while also verifying that the data file contains roughly the schema
you were expecting.  It is primarily intended for making automated imports
of data from non-automated or unstable sources, and providing human-readable
feedback about the validity of the data file.

=head1 ATTRIBUTES

=head2 input

This can be a file name or L<Path::Class> instance or file handle.  If a file
handle, it must be seekable in order to auto-detect the file format, I<or> you
may specify the decoder directly to avoid auto-detection.

=head2 decoder

This is either an instance of L<Data::RecordExtractor::Decoder>, or a class name,
or a partial class name to be appended as C<"Data::RecordExtractor::Decoder::$name">
or an arrayref or hashref of arguments to build the decoder.

Examples:

   'CSV'
   # becomes Data::RecordExtractor::Decoder::CSV->new()
   [ 'CSV', sep_char => "|" ]
   # becomes Data::RecordExtractor::Decoder::CSV->new(sep_char => "|")
   { CLASS => 'CSV', sep_char => "|" }
   # becomes Data::RecordExtractor::Decoder::CSV->new({ sep_char => "|" })

=head2 fields

An arrayref of L<Data::RecordExtractor::Field> objects (or hashrefs to
construct them with) which this module should search for within the L</input>.

=head2 record_class

Default is the special value 'HASH' for un-blessed hashref records.
The special value 'ARRAY' will result in arrayrefs with fields in the same
order they were specified in the L</fields> specification.
Setting it to anything else will return records created with
C<< $record_class->new(\%fields); >>

=head2 filters

List of filters which should be applied to the data records after they have
been pulled from the decoder but before they have been type-checked or passed
to the record constructor (if any).  Each element of the list should be a
coderef which receives a hashref and returns it, possibly modified.

=head2 static_field_order

Boolean, whether the L</fields> must be found in columns in the exact order
that they were specified.  Default is false.

=head2 header_row_at

Row number, or range of row numbers where the header must be found.
(All row numbers in this module are 1-based, to match end-user expectations.)
The default is C<[1,10]> to limit header scanning to the first 10 rows.
As a special case, if you are reading a source which lacks headers and you
trust the source to deliver the columns in the right order, you can set this
to undef if you also set C<< static_field_order => 1 >>.

=head2 on_unknown_columns

  on_unknown_columns => 'use'  # warn, and then use the table
  on_unknown_columns => 'next' # warn, and then look for another table which matches
  on_unknown_columns => 'die'  # fatal error
  on_unknown_columns => sub {
    my ($extractor, $col_headers)= @_;
    ...;
    return $opt;
  }

This determines handling for columns that aren't associated with any field.
The 'warn' and 'die' options will actually call carp/croak.  If you want to
investigate the situation yourself, pass a coderef which will receive the
list of columns that didn't match, and can decide what action to take.
If you use the callback, it suppresses the default warning, since you can
generate your own.
If you want to get cleaner default log messages, i.e. to show to users, see L</LOGGING>.

The default is 'use'.

=head2 on_blank_rows

  on_blank_rows => 'next' # warn, and then skip the rows
  on_blank_rows => 'last' # warn, and stop iterating the table
  on_blank_rows => 'die'  # fatal error
  on_blank_rows => 'use'  # actually try to return the blank rows as records
  on_blank_rows => sub {
    my ($extractor, $first_blank_rownum, $last_blank_rownum)= @_;
    ...;
    return $opt;
  }

This determines what happens when you've found the table, are extracting
records, and encounter a series of blank rows (defined as a row with no
printable characters in any field) followed by non-blank rows.
If you use the callback, it suppresses the default warning, since you can
generate your own.
If you want to get cleaner log messages, i.e. to show to users, see L</LOGGING>.

The default is 'next'.

=head2 on_validation_fail

  on_validation_fail => 'next'  # warn, and then skip the record
  on_validation_fail => 'use'   # warn, and then use the record anyway
  on_validation_fail => 'die'   # fatal error
  on_validation_fail => sub {
    my ($extractor, $record, $failed_fields)= @_;
    for my $field (@$failed_fields) {
      if ($record->{ $field->name } ...) {
        ...
      }
    }
    return $opt;
  }

This determines what happens when you've found the table, are extracting
records, and one row fails its validation.  In addition to deciding an option,
the callback gives you a chance to alter the record before 'use'ing it.
If you use the callback, it suppresses the default warning, since you can
generate your own.
If you want to get cleaner log messages, i.e. to show to users, see L</LOGGING>.

The default is 'die'.

=head2 logger

If undefined (the default) all log messages above 'info' will be emitted with
C<warn "$message\n">.  If set to an object, it should support an API of:

  trace,  is_trace
  debug,  is_debug
  info,   is_info
  warn,   is_warn
  error,  is_error

such as L<Log::Any> and may other perl logging modules use.  Finally, you can
set it to a coderef such as:

  my @messages;
  sub { my ($level, $message)= @_;
    push @messages, [ $level, $message ]
      if grep { $level eq $_ } qw( info warn error );
  };

for a simple way to capture the messages without involving a logging module.
And for extra convenience, you can set it to an arrayref which will receive
any message that would otherwise have gone to 'warn' or 'error'.

=cut

has input               => ( is => 'rw', required => 1 );
has _file_handle        => ( is => 'lazy' );
has _decoder_arg        => ( is => 'rw', init_arg => 'decoder' );
has decoder             => ( is => 'lazy', init_arg => undef );
has fields              => ( is => 'rw', required => 1, coerce => \&_coerce_field_list );
has record_class        => ( is => 'rw', required => 1, default => sub { 'HASH' } );
has filters             => ( is => 'rw' ); # list of coderefs to apply to the data
has static_field_order  => ( is => 'rw' ); # force order of columns
has header_row_at       => ( is => 'rw', default => sub { [1,10] } ); # row of header, or range to scan
has on_unknown_columns  => ( is => 'rw', default => sub { 'use' } );
has on_blank_rows       => ( is => 'rw', default => sub { 'next' } );
has on_validation_fail  => ( is => 'rw', default => sub { 'die' } );
has logger              => ( is => 'rw' );

sub _build__file_handle {
   my $self= shift;
   my $i= $self->input;
   return $i if ref($i) && (ref($i) eq 'GLOB' or ref($i)->can('read'));
   open(my $fh, '<', $i) or croak "open($i): $!";
   return $fh;
}

sub _build_decoder {
	my $self= shift;
	my $decoder_arg= $self->_decoder_arg;
	my ($class, @args);
	if (!$decoder_arg) {
		($class, @args)= $self->detect_input_format;
	}
	elsif (!ref $decoder_arg) {
		$class= $decoder_arg;
	}
	elsif (ref $decoder_arg eq 'HASH') {
		my %tmp= %$decoder_arg;
		$class= delete $tmp{CLASS}
			or ($class)= $self->detect_input_format
			or croak "require ->{CLASS} in decoder arguments";
		@args= %tmp;
	}
	elsif (ref $decoder_arg eq 'ARRAY') {
		($class, @args)= @$decoder_arg;
	}
	elsif (ref($decoder_arg)->can('iterator')) {
		return $decoder_arg;
	}
	else {
		croak "Can't create decoder from ".ref($decoder_arg);
	}
	$class= "Data::RecordExtractor::Decoder::$class"
		unless $class =~ /::/;
	require_module($class) or croak "$class does not exist or is not installed";
	return $class->new(
		file_name   => ($self->input eq $self->_file_handle? '' : $self->input),
		file_handle => $self->_file_handle,
		logger      => $self->logger,
		@args
	);
}

sub detect_input_format {
   my $self= shift;
   
   my $magic= '';
   my $input= $self->input || '';
   my ($suffix)= ($input =~ /\.([^.]+)$/);
   $suffix= defined $suffix? uc($suffix) : '';
   
   my $fh= $self->_file_handle;
   my $fpos= $fh->tell;
   if (defined $fpos && $fpos >= 0) {
      $fh->read($magic, 4096);
      $fh->seek($fpos, 0) or croak "seek: $!";
   }
   
   # Excel is obvious so check it first.  This handles cases where an excel file is
   # erroneously named ".csv" and sillyness like that.
   return ( 'XLSX' ) if $magic =~ /^PK/;
   return ( 'XLS'  ) if $magic =~ /^\xD0\xCF\x11\xE0/;
   
   # Else trust the file extension
   return $suffix if length $suffix;
   
   # Else probe some more...
   $log->debug("Probing file format because no filename suffix");
   length $magic or croak "Can't probe format. No filename suffix, and ".($fpos >= 0? "unseekable file handle" : "no content");
   
   my ($probably_csv, $probably_tsv)= (0,0);
   ++$probably_csv if $magic =~ /^["']?[\w ]+["']?,/;
   ++$probably_tsv if $magic =~ /^["']?[\w ]+["']?\t/;
   my $comma_count= ($magic =~ /,/g);
   my $tab_count= ($magic =~ /\t/g);
   my $eol_count= ($magic =~ /\n/g);
   ++$probably_csv if $comma_count > $eol_count and $comma_count > $tab_count;
   ++$probably_tsv if $tab_count > $eol_count and $tab_count > $comma_count;
   return 'CSV' if $probably_csv and $probably_csv > $probably_tsv;
   return 'TSV' if $probably_tsv and $probably_tsv > $probably_csv;
   
   croak "Can't determine file format";
}

sub _coerce_field_list {
	my ($list)= @_;
	defined $list and ref $list eq 'ARRAY' or croak "'fields' must be a non-empty arrayref";
	my @list= @$list; # clone it, to make sure we don't unexpectedly alter the caller's data
	for (@list) {
		if (!ref $_) {
			$_= Data::RecordExtractor::Field->new({ name => $_ });
		} elsif (ref $_ eq 'HASH') {
			my %args= %$_;
			# "isa" alias for the 'type' attribute
			$args{type}= delete $args{isa} if defined $args{isa} && !defined $args{type};
			$_= Data::RecordExtractor::Field->new(\%args)
		} else {
			croak "Can't coerce '$_' to a Field object"
		}
	}
	return \@list;
}

1;
