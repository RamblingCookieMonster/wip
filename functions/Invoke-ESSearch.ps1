function Invoke-ESSearch {
    <#
    .SYNOPSIS
        Invoke an ElasticSearch Bool search

    .DESCRIPTION
        Invoke an ElasticSearch Bool query

        * Generates bool search json (use debug to see before you run)
        * Invokes query against _search api
        * Uses scroll to paginate
        * Deletes scrolls after use

    .LINK
        https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html

    .PARAMETER Must
        Clauses to include in bool 'must' parameter

        Easy way:
        @{ key = 'ValueWithNoSpace' }
            Translates to:
            { "must": [ { "term": { "key": "ValueWithNoSpace"} } ]}

        @{ key = 'Value with space' }
            Translates to:
            { "must": [ { "match_phrase": { "key": "Value with space"} } ]}

        Flexible way:
        @{
            key = @{
                ESType = 'match' #e.g. match_phrase, term
                Value = 'some value to match'
            }
        }
            Translates to
            { "must": [ { "match": { "key": "some value to match"} } ]}

    .PARAMETER MustNot
        Clauses to include in bool 'must_not' parameter

        Easy way:
        @{ key = 'ValueWithNoSpace' }
            Translates to:
            { "must_not": [ { "term": { "key": "ValueWithNoSpace"} } ]}

        @{ key = 'Value with space' }
            Translates to:
            { "must_not": [ { "match_phrase": { "key": "Value with space"} } ]}

        Flexible way:
        @{
            key = @{
                ESType = 'match' #e.g. match_phrase, term
                Value = 'some value to match'
            }
        }
            Translates to
            { "must_not": [ { "match": { "key": "some value to match"} } ]}

    .PARAMETER Should
        Clauses to include in bool 'should' parameter

        Easy way:
        @{ key = 'ValueWithNoSpace' }
            Translates to:
            { "should": [ { "term": { "key": "ValueWithNoSpace"} } ]}

        @{ key = 'Value with space' }
            Translates to:
            { "should": [ { "match_phrase": { "key": "Value with space"} } ]}

        Flexible way:
        @{
            key = @{
                ESType = 'match' #e.g. match_phrase, term
                Value = 'some value to match'
            }
        }
            Translates to
            { "should": [ { "match": { "key": "some value to match"} } ]}

    .PARAMETER Filter
        Clauses to include in bool 'filter' parameter

        Easy way:
        @{ key = 'ValueWithNoSpace' }
            Translates to:
            { "filter": [ { "term": { "key": "ValueWithNoSpace"} } ]}

        @{ key = 'Value with space' }
            Translates to:
            { "filter": [ { "match_phrase": { "key": "Value with space"} } ]}

        Flexible way:
        @{
            key = @{
                ESType = 'match' #e.g. match_phrase, term
                Value = 'some value to match'
            }
        }
            Translates to
            { "filter": [ { "match": { "key": "some value to match"} } ]}

    .PARAMETER SingleValueESType
        Specifies a parameter to use for simplified must, must_not, filter, and should queries
        Defaults to match

        To clarify, using the following parameters:
            -SingleValueESType wildcard
            -Filter @{ 'event_data.TargetUserName' = 'wf*' }
        Will result in the following query DSL:
            { "filter": [ { "wildcard": { "event_data.TargetUserName": "wf*"} } ]}

    .PARAMETER Size
        Maximum number of hits to be returned with each batch of results

    .PARAMETER ScrollMinutes
        How long ElasticSearch should keep the search context alive

    .PARAMETER Index
        Limit query to one or more indices when specified

        Example:
           someindex            A single index
           oneindex, twoindex   Multiple indices
           *test                Wildcards suppor    ted
           _all                 All indices

    .PARAMETER BaseUri
        Elasticsearch BaseUri

    .PARAMETER KeepScrolls
        If specified, don't delete scroll_ids that we create

    .FUNCTIONALITY
        PowerShell Language
    #>
    [cmdletbinding()]
    param(
        [hashtable[]]$Must,
        [hashtable[]]$MustNot,
        [hashtable[]]$Should,
        [hashtable[]]$Filter,
        [int]$Size = 100,
        [int]$ScrollMinutes = 1,
        [string[]]$Index = $null,
        [switch]$KeepScrolls,
        [string]$SingleValueESType = 'match',  # term, match etc.

        [string]$BaseUri = $Script:ESConfig.BaseUri,

        [ValidateNotNull()]
        [System.Management.Automation.PSCredential]
        [System.Management.Automation.Credential()]
        $Credential = $Script:ESConfig.Credential
    )
    $IndexString = $Index -join ','
    # Build URIs to use
    $Uri = Join-Parts -Separator '/' -Parts $BaseUri, $IndexString, _search?scroll=${ScrollMinutes}m
    $ScrollUri = Join-Parts -Separator '/' -Parts $BaseUri, _search, scroll

    function Get-BoolParameter {
        param(
            [hashtable[]]$Hashes,
            [string]$ElasticSearchTypeKey = 'ESType' # e.g. match_phrase
        )
        $Statements = New-Object System.Collections.ArrayList
        foreach($Hash in $Hashes) {
            foreach($key in $Hash.Keys) {
                $HashValue = $Hash[$Key]
                Write-Verbose "Parsing hashtable key=[$Key] value=[$($HashValue | Out-String)] type=[$($HashValue.GetType())]"
                if($HashValue -is [hashtable] -or
                   ($HashValue.count -eq 1 -and $HashValue[0] -is [hashtable])) {
                    if($HashValue.ContainsKey('ESType')) {
                        $HashCopy = Copy-Object -InputObject $HashValue
                        $ESType = $HashValue.$ElasticSearchTypeKey
                        [void]$HashCopy.Remove($ElasticSearchTypeKey)
                    }
                    $Value = $HashValue.Value
                }
                else {
                    $ESType = $Null
                    $Value = $HashValue
                }
                # Pick some sane-ish defaults based on value
                if(-not $ESType) {
                    if($Value -match "\s") {$ESType = 'match_phrase'}
                    if($Value -notmatch "\s") {$ESType = $SingleValueESType}
                    if($Value -is [object[]]) {$ESType = 'terms'}
                }
                $Statement = @{
                    $ESType = @{ $Key = $Value }
                }
                [void]$Statements.add( $Statement )
            }
        }
        $Statements
    }
    $BoolParam = @{}
    if($Must) {
        [void]$BoolParam.add('must', $(Get-BoolParameter -Hashes $Must))
    }
    if($MustNot) {
        [void]$BoolParam.add('must_not', $(Get-BoolParameter -Hashes $MustNot))
    }
    if($Should) {
        [void]$BoolParam.add('should', $(Get-BoolParameter -Hashes $Should))
    }
    if($Filter) {
        [void]$BoolParam.add('filter', $(Get-BoolParameter -Hashes $Filter))
    }
    $Body = [pscustomobject]@{
        size = $Size
        query = @{
            bool = $BoolParam
        }
    } | ConvertTo-Json -Depth 10

    $Scrolls = New-Object System.Collections.ArrayList
    $Results = New-Object System.Collections.ArrayList
    $IRMParams = @{
        ContentType = 'application/json'
        Method = 'Post'
        ErrorAction = 'Stop'
    }
    Write-Debug "Invoking search with query:`n $($Body | Out-String)"
    # Initial query will give us a scrollid to use against a scroll API
    $e = Invoke-RestMethod @IRMParams -Uri $Uri -Body $Body
    $ScrollId = $e._scroll_id
    if(-not $ScrollId) {
        throw "No _scroll_id found in output:`n$($e | Format-List | Out-String)"
    }
    [void]$Scrolls.Add($ScrollId)
    if($e.hits.hits.count -gt 0) {
        Write-Verbose "Added [$($e.hits.hits.count) initial hits]"
        Write-Verbose "First Hit:`n$($e.hits.hits[0] | Out-String)"
        [void]$Results.AddRange($e.hits.hits)
    }

    while ($e._scroll_id -and $e.hits.hits.count -gt 0)
    {
        $Body = "{ `"scroll`": `"${ScrollMinutes}m`", `"scroll_id`": `"$ScrollId`"}"
        $e = Invoke-RestMethod @IRMParams -Uri $ScrollUri -Body $Body

        if($e.hits.hits.count -gt 0) {
            Write-Verbose "Added [$($e.hits.hits.count) more hits]"
            Write-Verbose "First Hit:`n$($e.hits.hits[0] | Out-String)"
            [void]$Results.AddRange($e.hits.hits)
        }
        $ScrollId = $e._scroll_id
        if(-not $ScrollId) {
            throw  "No _scroll_id found in output:`n$($e | Format-List | Out-String)"
        }
        [void]$Scrolls.Add($ScrollId)
    }
    $Results
    if($Scrolls -and -not $KeepScrolls) {
        $Scrolls = $Scrolls | Get-Unique
        Write-Verbose "Removing [$($Scrolls.count)] scrolls"
        $ScrollString = ($Scrolls | foreach-object {"`"$_`""}) -join ',`n'
        $Uri = Join-Parts -Separator '/' -Parts $BaseUri, _search, scroll
        $r = Invoke-RestMethod -Uri $Uri -Method Delete -ContentType 'application/json' -Body "{`"scroll_id`" : [ $ScrollString ] }"
        Write-Verbose $($r | Out-String)
    }
}