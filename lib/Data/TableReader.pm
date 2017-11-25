package Data::TableReader;

use Moo 2;
use Try::Tiny;
use Carp;
use List::Util 'max';
use Module::Runtime 'require_module';
use Data::TableReader::Field;
use Data::TableReader::Iterator;

# ABSTRACT: Extract records from "dirty" tabular data sources

=head1 SYNOPSIS

  # Find a row in the Excel file containing the headers
  #   "address", "city", "state", "zip" (in any order)
  # and then convert each row under that into a hashref of those fields.
  
  my $records= Data::TableReader>new(
      input => 'path/to/file.xlsx',
      fields => [qw( address city state zip )],
    )
    ->iterator->all;

but there's plenty of options to choose from...

  my $tr= Data::TableReader->new(
    # path or file handle
    # let it auto-detect the format (but can override that if we need)
    input => 'path/to/file.csv',
    
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
    
    # Our data provider is horrible; just ignore any nonsense we encounter
    on_blank_row => 'next',
    on_validation_fail => 'next',
    
    # Capture warnings and show to user who uploaded file
    log => \(my @messages)
  );
  
  my $records= $tr->iterator->all;
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

This is either an instance of L<Data::TableReader::Decoder>, or a class name,
or a partial class name to be appended as C<"Data::TableReader::Decoder::$name">
or an arrayref or hashref of arguments to build the decoder.

Examples:

  'CSV'
  # becomes Data::TableReader::Decoder::CSV->new()
  
  [ 'CSV', sep_char => "|" ]
  # becomes Data::TableReader::Decoder::CSV->new(sep_char => "|")
  
  { CLASS => 'CSV', sep_char => "|" }
  # becomes Data::TableReader::Decoder::CSV->new({ sep_char => "|" })

=head2 fields

An arrayref of L<Data::TableReader::Field> objects (or hashrefs to
construct them with) which this module should search for within the tables
(worksheets etc.) of L</input>.

=head2 record_class

Default is the special value C<'HASH'> for un-blessed hashref records.
The special value C<'ARRAY'> will result in arrayrefs with fields in the same
order they were specified in the L</fields> specification.
Setting it to anything else will return records created with
C<< $record_class->new(\%fields); >>

=head2 filters

Array of filters which should be applied to the data records after they have
been assembled but before they are passed to the record constructor (if any).
Each element of this array should be a coderef which receives a hashref and
returns it, possibly modified.

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
    my ($reader, $col_headers)= @_;
    ...;
    return $opt; # one of the above values
  }

This determines handling for columns that aren't associated with any field.
The "required" columns must all be found before it considers this setting, but once it has
found everything it needs to make this a candidate, you might or might not care about the
leftover columns.

=over

=item C<'use'>  (default)

You don't care if there are extra columns, just log warnings about them and proceed extracting
from this table.

=item C<'next'>

Extra columns mean that you didn't find the table you wanted.  Log the near-miss, and
keep searching additional rows or additional tables.

=item C<'die'>

This header is probably what you want, but you consider extra columns to be an error
condition.  Logs the details and calls C<croak>.

=item C<sub {}>

You can add your own logic to handle this.  Inspect the headers however you like, and then
return one of the above values.

=back

If you want to get cleaner default log messages, i.e. to show to users, see L</log>.

=head2 on_blank_rows

  on_blank_rows => 'next' # warn, and then skip the row(s)
  on_blank_rows => 'last' # warn, and stop iterating the table
  on_blank_rows => 'die'  # fatal error
  on_blank_rows => 'use'  # actually try to return the blank rows as records
  on_blank_rows => sub {
    my ($reader, $first_blank_rownum, $last_blank_rownum)= @_;
    ...;
    return $opt; # one of the above values
  }

This determines what happens when you've found the table, are extracting
records, and encounter a series of blank rows (defined as a row with no
printable characters in any field) followed by non-blank rows.
If you use the callback, it suppresses the default warning, since you can
generate your own.
If you want to get cleaner log messages, i.e. to show to users, see L</log>.

The default is C<'next'>.

=head2 on_validation_fail

  on_validation_fail => 'next'  # warn, and then skip the record
  on_validation_fail => 'use'   # warn, and then use the record anyway
  on_validation_fail => 'die'   # fatal error
  on_validation_fail => sub {
    my ($reader, $failures, $values, $context)= @_;
    for (@$failures) {
      my ($field, $value_index, $message)= @$_;
      ...
      # $field is a Data::TableReader::Field
      # $values->[$value_index] is the string that failed validation
      # $message is the error returned from the validation function
      # $context is a string describing the source of the row, like "Row 5"
      # You may modify $values to alter the record that is about to be created
    }
    # Clear the failures array to suppress warnings, if you actually corrected
    # the validation problems.
    @$failures= () if $opt eq 'use';
    # return one of the above constants to tell the iterator what to do next
    return $opt;
  }

This determines what happens when you've found the table, are extracting
records, and one row fails its validation.  In addition to deciding an option,
the callback gives you a chance to alter the record before C<'use'>ing it.
If you use the callback, it suppresses the default warning, since you can
generate your own.
If you want to get cleaner log messages, i.e. to show to users, see L</log>.

The default is 'die'.

=head2 log

If undefined (the default) all log messages above 'info' will be emitted with
C<warn "$message\n">.  If set to an object, it should support an API of:

  trace,  is_trace
  debug,  is_debug
  info,   is_info
  warn,   is_warn
  error,  is_error

such as L<Log::Any> and may other perl logging modules use.  You can also
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
sub field_list             { @{ shift->fields } }
has field_by_name       => ( is => 'lazy' );
has record_class        => ( is => 'rw', required => 1, default => sub { 'HASH' } );
has filters             => ( is => 'rw' ); # list of coderefs to apply to the data
has static_field_order  => ( is => 'rw' ); # force order of columns
has header_row_at       => ( is => 'rw', default => sub { [1,10] } ); # row of header, or range to scan
has header_row_combine  => ( is => 'rw', lazy => 1, builder => 1 );
has on_unknown_columns  => ( is => 'rw', default => sub { 'use' } );
has on_blank_row        => ( is => 'rw', default => sub { 'next' } );
has on_validation_fail  => ( is => 'rw', default => sub { 'die' } );
has log                 => ( is => 'rw', trigger => sub { shift->_clear_log } );

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
	$class= "Data::TableReader::Decoder::$class"
		unless $class =~ /::/;
	require_module($class) or croak "$class does not exist or is not installed";
	return $class->new(
		file_name   => ($self->input eq $self->_file_handle? '' : $self->input),
		file_handle => $self->_file_handle,
		log         => $self->_log,
		@args
	);
}

sub _coerce_field_list {
	my ($list)= @_;
	defined $list and ref $list eq 'ARRAY' or croak "'fields' must be a non-empty arrayref";
	my @list= @$list; # clone it, to make sure we don't unexpectedly alter the caller's data
	for (@list) {
		if (!ref $_) {
			$_= Data::TableReader::Field->new({ name => $_ });
		} elsif (ref $_ eq 'HASH') {
			my %args= %$_;
			# "isa" alias for the 'type' attribute
			$args{type}= delete $args{isa} if defined $args{isa} && !defined $args{type};
			$_= Data::TableReader::Field->new(\%args)
		} else {
			croak "Can't coerce '$_' to a Field object"
		}
	}
	return \@list;
}

sub _build_field_by_name {
	my $self= shift;
	# reverse list so first field of a name takes precedence
	{ map { $_->name => $_ } reverse @{ $self->fields } }
}

sub _build_header_row_combine {
	my $self= shift;
	# If headers contain "\n", we need to collect multiple cells per column
	max map { 1+($_->header_regex =~ /\\n/g) } $self->field_list;
}

has _log => ( is => 'lazy', clearer => 1 );
sub _build__log {
	my $dest= shift->log;
	!$dest? sub {
		my ($level, $msg, @args)= @_;
		return unless $level eq 'warn' or $level eq 'error';
		$msg= sprintf($msg, @args) if @args;
		warn $msg."\n";
	}
	: ref $dest eq 'ARRAY'? sub {
		my ($level, $msg, @args)= @_;
		return unless $level eq 'warn' or $level eq 'error';
		$msg= sprintf($msg, @args) if @args;
		push @$dest, [ $level, $msg ];
	}
	: ref($dest)->can('info')? sub {
		my ($level, $msg, @args)= @_;
		$level.='f' if @args;
		$dest->$level($msg, @args);
	}
	: croak "Don't know how to log to $dest";
}

=head1 METHODS

=head2 detect_input_format

   my ($class, @args)= $tr->detect_input_format( $filename, $head_of_file );

This is used internally to detect the format of a file, but you can call it manually if you
like.  The first argument (optional) is a file name, and the second argument (also optional)
is the first few hundred bytes of the file.  Missing arguments will be pulled from L</input>
if possible.  The return value is the best guess of module name and constructor arguments that
should be used to parse the file.  However, this doesn't guarantee such module actually exists
or is installed; it might just echo the file extension back to you.

=cut

sub detect_input_format {
	my ($self, $filename, $magic)= @_;
	# Detect filename if not supplied
	if (!defined $filename) {
		my $input= $self->input;
		$filename= '';
		$filename= "$input" if defined $input and (!ref $input || ref($input) =~ /path|file/i);
	}
	my ($suffix)= ($filename =~ /\.([^.]+)$/);
	$suffix= defined $suffix? uc($suffix) : '';
	# Load first block of file, unless supplied
	my $fpos;
	if (!defined $magic) {
		my $fh= $self->_file_handle;
		$fpos= tell $fh;
		if (defined $fpos && $fpos >= 0) {
			read($fh, $magic, 4096);
			seek($fh, $fpos, 0) or croak "seek: $!";
		} else {
			$magic= '';
		}
	}

	# Excel is obvious so check it first.  This handles cases where an excel file is
	# erroneously named ".csv" and sillyness like that.
	return ( 'XLSX' ) if $magic =~ /^PK(\x03\x04|\x05\x06|\x07\x08)/;
	return ( 'XLS'  ) if $magic =~ /^\xD0\xCF\x11\xE0/;

	# Else trust the file extension, because TSV with commas can be very similar to CSV with
	# tabs in the data.
	return $suffix if length $suffix;

	# Else probe some more...
	$self->_log->('debug',"Probing file format because no filename suffix");
	length $magic or croak "Can't probe format. No filename suffix, and "
		.($fpos >= 0? "unseekable file handle" : "no content");

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

=head2 find_table

  if ($tr->find_table) { ... }

Search through the input for the beginning of the records, identified by a header row matching
the various constraints defined in L</fields>.  If L</header_row_at> is undef, then this does
nothing and assumes success.

Returns a boolean of whether it succeeded.  This method does B<not> C<croak> on failure like
L</iterator> does, on the assumption that you want to handle them gracefully.
All diagnostics about the search are logged via L</log>.

=head2 col_map

This is a lazy attribute from table detection.  After calling L</find_table> you can inspect
which fields were found for each column via this method.  If called before C<find_table>, this
triggers table detection and throws an exception if one isn't found.

Returns an arrayref with one element for each column, each undefined or a reference to the
Field object it matched.

=head2 field_map

This is another lazy attribute from table detection, mapping from field name to column
index/indicies which the field will be loaded from.  If called before C<find_table>, this
triggers table detection and throws an exception if one isn't found.

Returns a hashref where key is the field name, and value is either a single column index, or
an arrayref of column indicies if the field is an L<array|Data::TableReader::Field/array> field.

=cut

has _table_found => ( is => 'rw', lazy => 1, builder => 1, clearer => 1, predicate => 1 );
sub _build__table_found {
	my $self= shift;
	my %loc= ( croak_on_fail => 1 );
	$self->_find_table($self->decoder->iterator, \%loc);
	\%loc;
}

sub find_table {
	my $self= shift;
	return 1 if $self->_has_table_found;
	my %loc;
	if ($self->_find_table($self->decoder->iterator, \%loc)) {
		$self->_table_found(\%loc);
		return 1;
	}
	return 0;
}

sub col_map                { shift->_table_found->{col_map}; }
sub field_map              { shift->_table_found->{field_map}; }

sub _find_table {
	my ($self, $data_iter, $stash)= @_;
	$stash ||= {};
	while (!$self->_find_table_in_dataset($data_iter, $stash)
		&& !defined $stash->{fatal}
		&& $data_iter->next_dataset)
	{}
	if ($stash->{col_map}) {
		# Calculate field map from col map
		my $col_map= $stash->{col_map};
		my %fmap;
		for my $i (0 .. $#$col_map) {
			next unless $col_map->[$i];
			if ($col_map->[$i]->array) {
				push @{ $fmap{$col_map->[$i]->name} }, $i;
			} else {
				$fmap{$col_map->[$i]->name}= $i;
			}
		}
		$stash->{field_map}= \%fmap;
		# And record the stream position of the start of the table
		$stash->{first_record_pos}= $data_iter->tell;
		$stash->{data_iter}= $data_iter;
		return $stash;
	}
	else {
		my $err= $stash->{fatal} || "Can't locate valid header";
		$self->_log->('error', $err);
		croak $err if $stash->{croak_on_fail};
		return undef;
	}
}

sub _find_table_in_dataset {
	my ($self, $data_iter, $stash)= @_;
	# If header_row_at is undef, then there is no header.
	# Ensure static_field_order, then set up columns.
	my @fields= $self->field_list;
	my $header_at= $self->header_row_at;
	if (!defined $header_at) {
		unless ($self->static_field_order) {
			$stash->{fatal}= "You must enable 'static_field_order' if there is no header row";
			return;
		}
		$stash->{col_map}= \@fields;
		return 1;
	}
	
	# If headers contain "\n", we need to collect multiple cells per column
	my $row_accum= $self->header_row_combine;
	
	my ($start, $end)= ref $header_at? @$header_at : ( $header_at, $header_at );
	my @rows;
	
	# If header_row_at doesn't start at 1, seek forward
	push @rows, $data_iter->() for 1..$start-1;
	
	# Scan through the rows of the dataset up to the end of header_row_at, accumulating rows so that
	# multi-line regexes can match.
	for ($start .. $end) {
		my $vals= $data_iter->() or last; # if undef, we reached end of dataset
		if ($row_accum > 1) {
			push @rows, $vals;
			splice @rows, 0, @rows-$row_accum; # only need to retain $row_accum number of rows
			$vals= [ map { my $c= $_; join("\n", map $_->[$c], @rows) } 0 .. $#{$rows[-1]} ];
		}
		$stash->{context}= $data_iter->position.': ';
		$stash->{col_map}= $self->static_field_order?
			# If static field order, look for headers in sequence
			$self->_match_headers_static($vals, $stash)
			# else search for each header
			: $self->_match_headers_dynamic($vals, $stash);
		return 1 if $stash->{col_map};
		return if $stash->{fatal};
	}
	$self->_log->('warn','No row in dataset matched full header requirements');
	return;
}

sub _match_headers_static {
	my ($self, $header, $cache)= @_;
	my $fields= $self->fields;
	for my $i (0 .. $#$fields) {
		next if $header->[$i] =~ $fields->[$i]->header_regex;
		
		# Field header doesn't match.  Start over on next row.
		$self->_log->('debug','%sMissing field %s', $cache->{context}||'', $fields->[$i]->name);
		return;
	}
	# found a match for every field!
	return $fields;
}

sub _match_headers_dynamic {
	my ($self, $header, $stash)= @_;
	my $context= $stash->{context} || '';
	my %col_map;
	my $fields= $self->fields;
	my $free_fields=    $stash->{free_fields} ||= [
		sort { $a->required? -1 : $b->required? 1 : 0 }	# Sort required first, to fail faster on non-matching rows
		grep { !$_->follows_list } @$fields
	];
	my $follows_fields= $stash->{follows_fields} ||= [
		grep { $_->follows_list } @$fields
	];
	for my $f (@$free_fields) {
		my $hr= $f->header_regex;
		my @found= grep { $header->[$_] =~ $hr } 0 .. $#$header;
		if (@found == 1) {
			if ($col_map{$found[0]}) {
				$self->_log->('warn','%sField %s and %s both match',
					$context, $f->name, $col_map{$found[0]}->name);
				return;
			}
			$col_map{$found[0]}= $f;
		}
		elsif (@found > 1) {
			if ($f->array) {
				# Array columns may be found more than once
				$col_map{$_}= $f for @found;
			} else {
				$self->_log->('warn','%sField %s matches more than one column',
					$context, $f->name);
				return;
			}
		}
		elsif ($f->required) {
			$self->_log->('debug','%sNo match for required field %s', $context, $f->name);
			return;
		}
		# else Not required, and not found
	}
	# Need to have found at least one column (even if none required)
	unless (keys %col_map) {
		$self->_log->('debug','%sNo fields matched', $context);
		return;
	}
	# Now, check for any of the 'follows' fields, some of which might also be 'required'.
	if (@$follows_fields) {
		my %following;
		my %found;
		for my $i (0 .. $#$header) {
			if ($col_map{$i}) {
				%following= ( $col_map{$i}->name => $col_map{$i} );
			} else {
				my $val= $header->[$i];
				my @match;
				for my $f (@$follows_fields) {
					next unless grep $following{$_}, $f->follows_list;
					push @match, $f if $val =~ $f->header_regex;
				}
				if (@match == 1) {
					if ($found{$match[0]} && !$match[0]->array) {
						$self->_log->('error','%sField %s matches multiple columns', 
							$context, $match[0]->name);
						return;
					}
					$col_map{$i}= $match[0];
					$found{$match[0]}= $i;
					$following{$match[0]->name}= $match[0];
				}
				elsif (@match > 1) {
					$self->_log->('error','%sField %s and %s both match column %d',
						$context, $match[0]->name, $match[1]->name, $i+1);
					return;
				}
				else {
					%following= ();
				}
			}
		}
		# Check if any of the 'follows' fields were required
		if (my @unfound= grep { !$found{$_} && $_->required } @$follows_fields) {
			$self->_log->('debug','%sNo match for required field %s', $context, $_->name)
				for @unfound;
		}
	}
	# Now, if there are any un-claimed columns, handle per 'on_unknown_columns' setting.
	my @unclaimed= grep { !$col_map{$_} } 0 .. $#$header;
	if (@unclaimed) {
		my $act= $self->on_unknown_columns;
		my $unknown_list= join(', ', map $header->[$_], @unclaimed);
		$act= $act->($self, $header, \@unclaimed) if ref $act eq 'CODE';
		if ($act eq 'use') {
			$self->_log->('warn','%sIgnoring unknown columns: %s', $context, $unknown_list);
		} elsif ($act eq 'next') {
			$self->_log->('warn','%sWould match except for unknown columns: %s',
				$context, $unknown_list);
		} elsif ($act eq 'die') {
			$stash->{fatal}= "${context}Header row includes unknown columns: $unknown_list";
		} else {
			$stash->{fatal}= "Invalid action '$act' for 'on_unknown_columns'";
		}
		return if $stash->{fatal};
	}
	return [ map $col_map{$_}, 0 .. $#$header ];
}

=head2 iterator

  my $iter= $tr->iterator;
  while (my $rec= $iter->()) { ... }

Create an iterator.  If the table has not been located, then find it and C<croak> if it
can't be found.  Depending on the decoder and input filehandle, you might only be able to
have one instance of the iterator at a time.

The iterator derives from L<Data::TableReader::Iterator> but also has a method "all" which
returns all records in an arrayref.

  my $records= $tr->iterator->all;

=cut

has _iterator => ( is => 'rw', weak_ref => 1 );
sub iterator {
	my $self= shift;
	if ($self->_iterator) {
		# old one is still out there, so need a new data handle
		my $data_iter= $self->decoder->iterator;
		$data_iter->seek($self->_data_iter_record_start);
		$self->_data_iter($data_iter);
	}
	$self->_iterator(my $i= $self->_build_iterator);
	return $i;
}

sub _make_validation_callback {
	my ($self, $field, $index)= @_;
	my $t= $field->type;
	ref $t eq 'CODE'? sub {
		my $e= $t->($_[0][$index]);
		defined $e? ([ $field, $index, $e ]) : ()
	}
	: $t->can('validate')? sub {
		my $e= $t->validate($_[0][$index]);
		defined $e? ([ $field, $index, $e ]) : ()
	}
	: croak "Invalid type constraint $t on field ".$field->name;
}

sub _build_iterator {
	my $self= shift;
	my $fields= $self->fields;
	my $data_iter= $self->_table_found->{data_iter};
	my $col_map=   $self->_table_found->{col_map};
	my $field_map= $self->_table_found->{field_map};
	my @row_slice; # one column index per field, and possibly more for array_val_map
	my @arrayvals; # list of source index and destination index for building array values
	my @field_names; # ordered list of field names where row slice should be assigned
	my @trim_idx;  # list of array indicies which should be whitespace-trimmed.
	my @blank_val; # blank value per each fetched column
	my @type_check;# list of 
	my $class;     # optional object class for the resulting rows

	# If result is array, the slice of the row must match the position of the fields in the
	#  $self->fields array.  If a field was not found it will get an undef for that slot.
	# It also results in an undef for secondary fields of the same name as the first.
	if ($self->record_class eq 'ARRAY') {
		my %remaining= %$field_map;
		@row_slice= map {
			my $src= delete $remaining{$_->name};
			defined $src? $src : 0x7FFFFFFF
			} @$fields;
	}
	# If result is anything else, then only slice out the columns that are used for the fields
	# that we located.
	else {
		$class= $self->record_class
			unless 'HASH' eq $self->record_class;
		@field_names= keys %$field_map;
		@row_slice= values %$field_map;
	}
	# For any field whose value is an array of more that one source column,
	#  encode those details in @arrayvals, and update @row_slice and @trim_idx accordingly
	for (0 .. $#row_slice) {
		if (!ref $row_slice[$_]) {
			my $field= $col_map->[$row_slice[$_]];
			push @trim_idx, $_ if $field->trim;
			push @blank_val, $field->blank;
			push @type_check, $self->_make_validation_callback($field, $_)
				if $field->type;
		}
		else {
			# This field is an array-value, so add the src columns to @row_slice
			#  and list it in @arrayvals, and update @trim_idx if needed
			my $src= $row_slice[$_];
			$row_slice[$_]= 0x7FFFFFFF;
			my $from= @row_slice;
			push @row_slice, @$src;
			push @arrayvals, [ $_, $from, scalar @$src ];
			for ($from .. $#row_slice) {
				my $field= $col_map->[$row_slice[$_]];
				push @trim_idx, $_ if $field->trim;
				push @blank_val, $field->blank;
				push @type_check, $self->_make_validation_callback($field, $_)
					if $field->type;
			}
		}
	}
	@arrayvals= reverse @arrayvals;
	my @filters= @{ $self->filters || [] };
	my ($n_blank, $first_blank, $eof);
	my $sub= sub {
		again:
		# Pull the specific slice of the next row that we need
		my $row= !$eof && $data_iter->(\@row_slice)
			or ++$eof && return undef;
		# Apply 'trim' to any column whose field requested it
		for (grep { defined } @{$row}[@trim_idx]) {
			$_ =~ s/\s+$//;
			$_ =~ s/^\s+//;
		}
		# Apply 'blank value' to every column which is zero length
		$n_blank= 0;
		$row->[$_]= $blank_val[$_]
			for grep { (!defined $row->[$_] || !length $row->[$_]) && ++$n_blank } 0..$#$row;
		# If all are blank, then handle according to $on_blank_row setting
		if ($n_blank == @$row) {
			$first_blank ||= $data_iter->position;
			goto again;
		} elsif ($first_blank) {
			unless ($self->_handle_blank_row($first_blank, $data_iter->position)) {
				$eof= 1;
				return undef;
			}
			$first_blank= undef;
		}
		# Check type constraints, if any
		if (@type_check) {
			if (my @failed= map $_->($row), @type_check) {
				$self->_handle_validation_fail(\@failed, $row, $data_iter->position.': ')
					or goto again;
			}
		}
		# Collect all the array-valued fields from the tail of the row
		$row->[$_->[0]]= [ splice @$row, $_->[1], $_->[2] ] for @arrayvals;
		# stop here if the return class is 'ARRAY'
		unless (@field_names) {
			$_->($row) for @filters;
			return $row;
		}
		# Convert the row to a hashref
		my %rec;
		@rec{@field_names}= @$row;
		# Apply any filters
		$_->(\%rec) for @filters;
		# Construct a class, if requested, else return hashref
		return $class? $class->new(\%rec) : \%rec;
	};
	return Data::TableReader::_RecIter->new(
		$sub, { data_iter => $data_iter, reader => $self },
	);
}

sub _handle_blank_row {
	my ($self, $first, $last)= @_;
	my $act= $self->on_blank_row;
	$act= $act->($self, $first, $last)
		if ref $act eq 'CODE';
	if ($act eq 'next') {
		$self->_log->('warn', 'Skipping blank rows from %s until %s', $first, $last);
		return 1;
	}
	if ($act eq 'last') {
		$self->_log->('warn', 'Ending at blank row %s', $first);
		return 0;
	}
	if ($act eq 'die') {
		my $msg= "Encountered blank rows at $first..$last";
		$self->_log->('error', $msg);
		croak $msg;
	}
	croak "Invalid value for 'on_blank_row': \"$act\"";
}

sub _handle_validation_fail {
	my ($self, $failures, $values, $context)= @_;
	my $act= $self->on_validation_fail;
	$act= $act->($self, $failures, $values, $context)
		if ref $act eq 'CODE';
	my $errors= join(', ', map $_->[0]->name.': '.$_->[2], @$failures);
	if ($act eq 'next') {
		$self->_log->('warn', "%sSkipped for data errors: %s", $context, $errors) if $errors;
		return 0;
	}
	if ($act eq 'use') {
		$self->_log->('warn', "%sPossible data errors: %s", $context, $errors) if $errors;
		return 1;
	}
	if ($act eq 'die') {
		my $msg= "${context}Invalid record: $errors";
		$self->_log->('error', $msg);
		croak $msg;
	}
}

BEGIN { @Data::TableReader::_RecIter::ISA= ( 'Data::TableReader::Iterator' ) }
sub Data::TableReader::_RecIter::all {
	my $self= shift;
	my (@rec, $x);
	push @rec, $x while ($x= $self->());
	return \@rec;
}
sub Data::TableReader::_RecIter::position {
	shift->_fields->{data_iter}->position(@_);
}
sub Data::TableReader::_RecIter::progress {
	shift->_fields->{data_iter}->progress(@_);
}
sub Data::TableReader::_RecIter::tell {
	shift->_fields->{data_iter}->tell(@_);
}
sub Data::TableReader::_RecIter::seek {
	shift->_fields->{data_iter}->seek(@_);
}
sub Data::TableReader::_RecIter::next_dataset {
	shift->_fields->{reader}->_log
		->('warn',"Searching for supsequent table headers is not supported yet");
	return 0;
}

1;