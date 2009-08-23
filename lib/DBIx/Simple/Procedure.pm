package DBIx::Simple::Procedure;

use warnings;
use strict;
use DBIx::Simple;

=head1 NAME

DBIx::Simple::Procedure - An Alternative To SQL Stored Procedures using DBIx::Simple

=head1 VERSION

Version 1.32

=cut

our $VERSION = '1.32';
our @properties = caller();

=head1 SYNOPSIS

This module allow your program to process text files containing one or many commands that execute SQL statements sequentially. Please keep in mind that DBIx::Simple::Procedure
is an alternative to database stored procedures and not a replacement or emulation of them. Essentially it is an interface to execute and return data from multiple queries.

Here is an example of how to setup and process a (sql) text file.

    # DBIx::Simple::Procedure uses DBIx::Simple and provides an accessor through the DBIx::Simple::Procedure->{dbix} hash reference.
    use DBIx::Simple::Procedure;
    my $db = DBIx::Simple::Procedure->new($path_to_sqlfiles, 'dbi:SQLite:dbname=file.dat'); # will error out using DBIx::Simple if a connection error occurs.
    
    # the queue function takes one parameter (a text file) that contains DBIx::Simple::Procedure sql commands.
    # the process_queue function processes all queued sql statements using the parameters passed to it, similar to the execute function of DBI.
    $db->queue($sql_file)->process_queue(@sql_parameters);
    
    # the cache function returns an array of resultsets, or the resultset of the index passed, return by the select statements encountered in the sql file.
    # Note! files included using the "include" command will not have there resultsets cached, even if a "capture" command is encountered, only the select statement(s) found in the initial sql file are cached in the order they are encountered.
    foreach my $result (@{$db->cache(0)}){
        # do something with the records of the first resultset
        $result->{...};
    }

=head1 SQL FILE SYNTAX

The (sql) procedural text file to be processed may contain any text you desire, e.g. comments and other markup. DBIx::Simple::Procedure only reacts to command lines (commands).
These instructions (or commands) must be placed on its own line and be prefixed with an exclamation point, a space, the command, another space, and the statement to be evaluated.
E.g. "! execute select * from foo".
Multiple commands can be used in a single sql file.
Note! multi-line sql statements not supported in this release.

SQL File Commands:
    
    ! execute: This command simply execute the supplied sql statement.
    ! capture: This is an execute command who's dataset will be cached (stored) for later use. Note! This command can only be used with a select statement.
    ! replace: This is an execute command that after successfully executed, replaces the scope parameters with data from the last row in its dataset. Note! This command can only be used with a select statement.
    ! include: This command processes the supplied sql file in a sub transaction. Note! Included sql file processing is isolated from the current processing. Any capture commands encountered in the included sql files will not cache the dataset.
    ! proceed: This command should be read "proceed if" because it evaluates the string passed (perl code) for truth, if true, it continues if false it skips to the next proceed command or until the end of the sql file.
    ! ifvalid: This command is a synonym for proceed.
    ! storage: This command does absolutely nothing except store the sql statement in the commands list (queue) for processing individually from within the perl code with a method like process_command.
    ! declare: This command is effectively equivalent to the select .. into sql sytax and uses an sql select statement to add vairables to the scope for processing (e.g. ! declare select `name` from `foo` where `id` = $0)
               can be used in other instructions as $!name, e.g. ! execute update `foo` set `name` = $!name where `id` = $0.
    ! forward: This command takes an index and jumps to that command line and continues.
    ! process: This command takes an index and executes that command line.
    ! examine: This command is used for debugging, it errors out with the compiled statement passed to it.
    
    Example: ! execute select * from `foo` where `id` = $0

=head1 EXAMPLE

# @@@@@@@@@@ tables/group/insert.sql
! include tables/group/create.sql
! proceed 1 == 2
! execute insert into `group` values (null, concat_ws(' ', 'Test A', $0))
! proceed 1 == 1
! execute insert into `group` values (null, concat_ws(' ', 'Test B', $0))
! proceed 1
! capture select * from `group`
# @@@@@@@@@@ tables/group/create.sql
! execute create table if not exists `group` (`id` int(11) auto_increment, `info` varchar(255) not null, primary key(`id`) )
! execute truncate table `group`

# @@@@@@@@@@ test.pl
use FindBin;
use DBIx::Simple::Procedure;

# connecting to a mysql database
my $fs = "$FindBin::Bin/";
my $db = DBIx::Simple::Procedure->new(
    $fs,
    'dbi:mysql:database=foo', # dbi source specification
    '', '',                     # username and password
) or die DBIx::Simple::Procedure->error;

$db->queue('tables/group/insert.sql')->process_queue('One fish, two fish, red fish, blue fish.');
# or
$db->queue('tables/group/insert.sql')->process_command(4,'One fish, two fish, red fish, blue fish.'); # execute the 5th command in the tables/group/insert.sql file.

foreach my $result (@{$db->cache(0)}){
    # $result->{...};
}

=head1 METHODS

=cut

=head2 new
    The new method instantiates a new L<DBIx::Simple> and DBIx::Simple::Procedure object and accepts all parameters required by L<DBIx::Simple>.
=cut

sub new {
    my ($class, $path, @connect_options) = @_;
    my $self = {};
    bless $self, $class;
    $self->{sets} = [];
    $self->{path} = $path;
    $self->{dbix} = DBIx::Simple->connect(@connect_options) or die DBIx::Simple->error;
    $self->_load_commands;
    return $self;
}

=head2 _load_commands
    The _load_commands method is an internal method for build the commands dispatch table.
=cut

sub _load_commands {
    my $self = shift;
    
    # identify commands that can only contain select statements
    $self->{select_required} = ['capture', 'replace', 'declare'];
    
    #! capture: stores the resultset for later usage
    $self->{commands}->{capture} = sub {
        my ($statement, @parameters) = @_;
        $self->{processing}->{resultset} = $self->_execute_query($statement, @parameters);
        $self->{sets}->[@{$self->{sets}}] = $self->{processing}->{resultset}->hashes;
    };
    
    #! execute: execute sql commands only, nothing else, nothing fancy
    $self->{commands}->{execute} = sub {
        my ($statement, @parameters) = @_;
        $self->{processing}->{resultset} = $self->_execute_query($statement, @parameters);
    };
    
    #! proceed: evaluates the statement passed (perl code) for truth, if true, it continues if false it skips to the next proceed command or until the end of the sql file.
    $self->{commands}->{proceed} = sub {
        my ($statement, @parameters) = @_;
        if (@parameters) {
            foreach my $parameter (@parameters) {
                $_ = '' if !$_;
                $statement =~ s/\?/$_/;
            }
        }
        $self->{processing}->{skip_switch} = eval $statement ? 0 : 1;
    };
    
    #! ifvalid: a synonym for proceed
    $self->{commands}->{ifvalid} = $self->{commands}->{proceed};
    $self->{commands}->{validif} = $self->{commands}->{proceed};
    
    #! replace: replaces parameters with the data from the last row of the resultset
    $self->{commands}->{replace} = sub {
        my ($statement, @parameters) = @_;
        $self->{processing}->{resultset} = $self->_execute_query($statement, @parameters);
        $self->{processing}->{parameters} = @{$self->{processing}->{resultset}->array};
    };
    
    #! include: processes another (sql) text file
    $self->{commands}->{include} = sub {
        my ($statement, @parameters) = @_;
        my ($sub_sqlfile, $placeholders) = split /\s/, $statement;
        DBIx::Simple::Procedure->new($self->{path}, $self->{dbix}->{dbh})->queue($sub_sqlfile)->process_queue(@parameters);
    };
    
    #! storage: stores sql statements for later
    $self->{commands}->{storage} = sub {
        my ($statement, @parameters) = @_;
    };
    
    #! declare: uses an sql select statement to add vairables to the scope for processing
    $self->{commands}->{declare} = sub {
        my ($statement, @parameters) = @_;
        $self->{processing}->{resultset} = $self->_execute_query($statement, @parameters);
        my $results = $self->{processing}->{resultset}->hash;
        if ($results) {
            my %params = %{$results};
            while ( my ($key, $val) = each %params ) {
                $self->{processing}->{custom_parameters}->{$key} = $val;
            }
        }
    };
    
    #! forward: changes the queue position, good for looping
    $self->{commands}->{forward} = sub {
        my ($statement, @parameters) = @_;
        $self->{cursor} = $statement;
    };
    
    #! process: executes a command in the queue by index number
    $self->{commands}->{process} = sub {
        my ($statement, @parameters) = @_;
        $self->process_command($statement, @parameters);
    };
    
    #! examine: dumps the passed sql statement to the screen (should not be left in the sql file)
    $self->{commands}->{examine} = sub {
        my ($statement, @parameters) = @_;
        my $db = $self->{dbix}->{dbh};
        foreach my $parameter (@parameters) {
            my $placeholder = $db->quote($parameter);
            $statement =~ s/\?/$placeholder/;
        }
        die $self->_error( $statement );
    };
}

=head2 _execute_query
    The _execute_query method is an internal method for executing queries against the databse in a standardized fashion.
=cut

sub _execute_query {
    my ($self, $statement, @parameters) = @_;
    my $resultset = $self->{dbix}->query( $statement, @parameters ) or die $self->_error(undef, @parameters);
    return $resultset;
}

=head2 _error
    The die method is an internal method that dies with a standardized error message.
=cut

sub _error {
    my ($self, $message, @parameters) = @_;
    return ref($self) .
    " - sql file $self->{file} processing failed at the execution of command number " .
    ( $self->{cursor} || '0') .
    " [" . $self->{cmds}->[$self->{cursor}]->{command} . "] " .
    ( $self->{cmds}->[$self->{cursor}]->{statement} ? ( "and statement (" . substr($self->{cmds}->[$self->{cursor}]->{statement}, 0, 20) . "...) " ) : " " ) .
    ( @parameters ? ( "using " . join( ', ', @parameters ) . " " ) : "" ) . "at $properties[1]" .
    " on line $properties[2], " .
    ( $message || $self->{dbix}->error || "Check the sql file for errors" ) . ".";
}

=head2 queue
    The queue function parses the passed (sql) text file and build the list of sql statements to be executed and how.
=cut

sub queue {
    my ($self, $sqlfile) = @_;
    my (@statements);
    $self->{cmds} = '';
    
    # set caller data for error reporting
    @properties = caller();
    @statements = $self->_parse_sqlfile($sqlfile);
    $self->{cmds} = \@statements;
    return $self;
}

=head2 process_queue
    The process_queue function sequentially processes the recorded commands found the (sql) text file.
=cut

sub process_queue {
    my ($self, @parameters) = @_;
    # set caller data for error reporting
    @properties = caller();
    $self->{processing}->{parameters} = \@parameters;
    $self->{processing}->{skip_switch} = 0;
    $self->{cursor} = 0; 
    if (@{$self->{cmds}}) {
        # process sql commands 
        for (my $i = 0; $self->{cursor} < @{$self->{cmds}}; $i++) {
            my $cmd = $self->{cmds}->[$self->{cursor}];
            if ( grep($cmd->{command} eq $_, keys %{$self->{commands}}) )
            {
                # process command
                $self->_processor($cmd);
                $self->{cursor}++;
            }
        }
        return $self->{processing}->{resultset};
    }
    else {
        die $self->_error( "File has no commands to process" );
    }
    return $self;
}

=head2 _processor
    The _processor method is an internal methoed that when passed a command hashref, processes the command.
=cut

sub _processor {
    my ($self, $cmdref) = @_;
    my $command = $cmdref->{command};
    my $statement = $cmdref->{statement};
    no warnings;
    
    # replace statement placeholders with actual "?" placeholders while building the statement params list
    # my @statement_parameters = map { $self->{processing}->{parameters}[$_] } $statement =~ m/\$(\d+)/g;
    # $self->{processing}->{statement_parameters} = \@statement_parameters;
    # $statement =~ s/\$\d+/\?/g;
    
    # reset statement parameters
    $self->{processing}->{statement_parameters} = ();
    
    # replace statement placeholders with actual "?" placeholders while building the statement params list using passed or custom parameters
    while ($statement =~ m/(\$\!([a-z0-9A-Z\_\-]+))|(\$(\d+(?!\w)))/) {
        my $custom = $2;
        my $passed = $4;
        # if the found param is a custom param
        if (defined $custom) {
            push @{$self->{processing}->{statement_parameters}}, $self->{processing}->{custom_parameters}->{$custom};
            $statement =~ s/\$\!$custom/\?/;
        }
        # if the found param is a passed-in param
        if (defined $passed) {
            push @{$self->{processing}->{statement_parameters}}, $self->{processing}->{parameters}[$passed];
            $statement =~ s/\$$passed/\?/;
        }
    }
    
    if ($self->{processing}->{skip_switch} && $command ne "proceed")
    {
        # skip command while skip_switch is turned on
        return;    
    }
    else
    {
        # execute command
        $self->{commands}->{$command}->($statement, @{$self->{processing}->{statement_parameters}});
        return $self->{processing}->{resultset};
    }
}

=head2 cache
    The cache method accesses an arrayref of resultsets that were captured using the (sql file) capture command and return the resultset of the index passed to it or an empty arrayref.
=cut

sub cache {
    my ($self, $index) = @_;
    return defined $self->{sets}->[$index] ? $self->{sets}->[$index] : [];
    # return number of cached resultsets if index is not passed.
    return @{$self->{sets}};
}

=head2 command
    The command method is used to queue a command to be processed later by the process_queue method. Takes two arguments, "command" and "sql statement", e.g. command('execute', 'select * from foo').
=cut

sub command {
    my ($self, $command, $statement) = @_;
    my @statements = @{$self->{cmds}};
    push @statements, { "command" => "$command", "statement" => "$statement" };
    $self->{cmds} = \@statements;
    return $self;
}

=head2 process_command
    The (*experimental*) process_command method allows you to process the indexed sql satements from your sql file individually. It take one argument, the index of the command as it is encountered in the sql file and tries returns a resultset.
=cut

sub process_command {
    my ($self, $index, @parameters) = @_;
    my $cmd = $self->{cmds}->[$index];
    if ( grep($cmd->{command} eq $_, keys %{$self->{commands}}) )
    {
        # process command
        $self->{processing}->{parameters} = \@parameters if @parameters;
        return $self->_processor($cmd);
    }
}
=head2 clear
    The clear method simply clears the cache (resultset store)
=cut

sub clear {
    my $self = shift;
    $self->{cmds} = '';
    return $self;
}

=head2 _parse_sqlfile
    The _parse_sqlfile scans the passed (sql) text file and returns a list of sql statement queue objects.
=cut

sub _parse_sqlfile {
    my ($self, $sqlfile) = @_;
    my (@lines, @statements);
    # open file and fetch commands
    $self->{file} = $sqlfile;
    open (SQL, "$self->{path}$sqlfile") || die $self->_error( "Could'nt open $self->{path}$sqlfile sql file" );
    push @lines, $_ while(<SQL>);
    close SQL || die $self->_error( "Could'nt close $self->{path}$sqlfile sql file" );
    # attempt to parse commands
    foreach my $command (@lines) {
        if ($command =~ /^\!/) {
            my @commands = $command =~ /^\!\s(\w+)\s(.*)/;
            if (grep ( $commands[0] eq $_, keys %{$self->{commands}})) {
                push @statements, { "command" => "$commands[0]", "statement" => "$commands[1]" };
            }
        }
    }
    # validate statements
    $self->_validate_sqlfile(@statements);
    return @statements;
}

=head2 _validate_sqlfile
    The _validate_sqlfile method make sure that the supplied (sql) text file conforms to its command(s) rules.
=cut

sub _validate_sqlfile {
    my ($self, @statements) = @_;
    # rule1: replace, and capture can only be used with select statements
    foreach my $statement (@statements) {
        if (grep ( $statement->{command} eq $_, @{$self->{select_required}})) {
            if (lc($statement->{statement}) !~ /^(\s+)?select/) {
                die $self->_error( "Validation of the sql file $self->{file} failed. The command ($statement->{command}) can only be used with an SQL (select) statement.", $statement->{statement});
            }
        }
    }
}

=head1 TODO

...

=head1 AUTHOR

Al Newkirk, C<< <al.newkirk at awnstudio.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-dbix-simple-procedure at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DBIx-Simple-Procedure>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.




=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DBIx::Simple::Procedure


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=DBIx-Simple-Procedure>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/DBIx-Simple-Procedure>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/DBIx-Simple-Procedure>

=item * Search CPAN

L<http://search.cpan.org/dist/DBIx-Simple-Procedure/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 COPYRIGHT & LICENSE

Copyright 2009 Al Newkirk.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1; # End of DBIx::Simple::Procedure
