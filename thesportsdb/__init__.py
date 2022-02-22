"""
=======================================================================
:PACKAGE:   thesportsdb
:AUTHOR:	 Tralah M Brian <briantralah@tralahtek.com>
:TWITTER: 	 @TralahM
:GITHUB: 	 <https://github.com/TralahM/thesportsdb>
:COPYRIGHT:  (c) 2020  TralahTek LLC.
:LICENSE: 	 GPLV3 , see LICENSE for more details.
:WEBSITE:	<https://www.tralahtek.com>
:CREATED: 	2020-08-01
=======================================================================

DESCRIPTION::  TheSportsDB API Python SDK
Unofficial Python SDK  package around TheSportsDB API .
An open, crowd-sourced database of sports artwork and metadata with a free API.

"""
from __future__ import absolute_import
import thesportsdb.events
import thesportsdb.countries
import thesportsdb.leagues
import thesportsdb.players
import thesportsdb.teams
import thesportsdb.sports
import thesportsdb.settings
import thesportsdb.request
import thesportsdb.seasons

__version__ = "0.1.1"
__author__ = "Tralah M Brian <https://github.com/TralahM/thesportsdb>"
__all__ = [
    countries,
    events,
    leagues,
    players,
    request,
    settings,
    sports,
    teams,
    seasons,
]

