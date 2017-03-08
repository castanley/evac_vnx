#!/usr/bin/perl

use strict;
use warnings;

use Text::CSV;
use Data::Dumper;

my $file = 'VNX.csv';
open(my $fh, '<', $file);
my @luns = <$fh>;
close($fh);

my %lun;

foreach my $lun (@luns) {
        my $csv = Text::CSV->new({binary => 1, eol => $/});
        $csv->parse($lun);
        my ($name,$id,$state,$gb,$owner,$map,$uid,$navol) = $csv->fields();
        next unless $navol;
        $uid =~ s/://g;
        push @{$lun{$navol}}, [ $name, $uid, $gb, $map ];
}

foreach my $nv (sort keys %lun) {
        my $lun_total;
        my @na_cmds;
        foreach my $rec ( @{$lun{$nv}} ) {
                $lun_total += $$rec[2];
        }
        my $vol_size = int($lun_total * 1.5);
        my $vol_create = sprintf "na-adcna volume create -vserver VSERVER -volume %s -aggregate AGGREGATE -size %sGB -foreground true", $nv, $vol_size;
        print "$vol_create\n";
        system($vol_create);
        foreach my $rec ( @{$lun{$nv}} ) {
                my $l = sprintf "/vol/%s/%s", $nv, $$rec[0];
                my $lun_create = sprintf "na-adcna lun create -vserver VSERVER -path /vol/%s/%s -size %dGB -ostype linux", $nv, $$rec[0], $$rec[2];
                print "$lun_create\n";
                system($lun_create);
                my $lun_map;
                if ( $$rec[3] =~ /VPLEX0?1/i ) {
                        $lun_map = sprintf "na-adcna lun map -vserver VSERVER /vol/%s/%s %s", $nv, $$rec[0], 'VPLEX1';
                } else {
                        $lun_map = sprintf "na-adcna lun map -vserver VSERVER /vol/%s/%s %s", $nv, $$rec[0], 'VPLEX2';
                }
                print "$lun_map\n";
                system($lun_map);
                print "getting lun serial for $l...";
                my ( $line ) = grep(/\/vol/,`na-adcna lun serial -x $l`);
                print " DONE!\n";
                chomp($line);
                my ( $serial ) = $line =~ /\S+\s+\S+\s+(\S+)/;
                print "Rediscovering Netapp... ";
                my @storage_volumes = `vplex_array_rediscover_netapp.exp`;
                print "DONE!\n";
                my ($sv_line) = grep(/$serial/i, @storage_volumes);
                my ($dvpd) = $sv_line =~ /\S+\s+(\S+)/;
                print "Claim and rename $dvpd - " . $$rec[0] . "...";
                system("vplex_claim_extent.exp $dvpd " . $$rec[0]);
                print "DONE!\n";
                my ($sext_line) = grep(/$$rec[1]/i, @storage_volumes);
                my ($ssv) = $sext_line =~ /(\S+)\s+\S+/;
                print "Get use hierarchy for $ssv ... ";
                my ($temp) = grep(/extent/, `vplex_show_use_hierarchy.exp $ssv`);
                $temp =~ s/\x1b\[[0-9;]*m//g;
                print "DONE! found $temp\n";
                my ($sext) = $temp =~ /extent:\s+(extent_\S+)\s+/;
                chomp($sext);
                my $mcmd = "vplex_start_extent_migration.exp $$rec[0] $sext extent_" . $$rec[0];
                print "$mcmd\n";
                system($mcmd);
        }
}

#print "\n", Dumper(\%lun), "\n";
