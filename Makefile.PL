use strict;
use warnings;
use inc::Module::Install;

WriteMakefile(
    NAME                => 'DBIx::Simple::Procedure',
    AUTHOR              => q{Al Newkirk <al.newkirk@awnstudio.com>},
    VERSION_FROM        => 'lib/DBIx/Simple/Procedure.pm',
    ABSTRACT_FROM       => 'lib/DBIx/Simple/Procedure.pm',
    LICENSE             => 'perl',
    PL_FILES            => {},
    PREREQ_PM => {
        'DBIx::Simple' => 0,
        'Test::More' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'DBIx-Simple-Procedure-*' },
);
