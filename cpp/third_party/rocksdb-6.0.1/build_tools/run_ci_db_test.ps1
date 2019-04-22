# This script enables you running RocksDB tests by running
# All the tests concurrently and utilizing all the cores
Param(
  [switch]$EnableJE = $false,  # Look for and use test executable, append _je to listed exclusions
  [switch]$RunAll = $false,    # Will attempt discover all *_test[_je].exe binaries and run all
                               # of them as Google suites. I.e. It will run test cases concurrently
                               # except those mentioned as $Run, those will run as individual test cases
                               # And any execlued with $ExcludeExes or $ExcludeCases
                               # It will also not run any individual test cases
                               # excluded but $ExcludeCasese
  [switch]$RunAllExe = $false, # Look for and use test exdcutables, append _je to exclusions automatically
                               # It will attempt to run them in parallel w/o breaking them up on individual
                               # test cases. Those listed with $ExcludeExes will be excluded
  [string]$SuiteRun = "",      # Split test suites in test cases and run in parallel, not compatible with $RunAll
  [string]$Run = "",           # Run specified executables in parallel but do not split to test cases
  [string]$ExcludeCases = "",  # Exclude test cases, expects a comma separated list, no spaces
                               # Takes effect when $RunAll or $SuiteRun is specified. Must have full
                               # Test cases name including a group and a parameter if any
  [string]$ExcludeExes = "",   # Exclude exes from consideration, expects a comma separated list,
                               # no spaces. Takes effect only when $RunAll is specified
  [string]$WorkFolder = "",    # Direct tests to use that folder. SSD or Ram drive are better options.
   # Number of async tasks that would run concurrently. Recommend a number below 64.
   # However, CPU utlization really depends on the storage media. Recommend ram based disk.
   # a value of 1 will run everything serially
  [int]$Concurrency = 8,
  [int]$Limit = -1 # -1 means do not limit for test purposes
)

# Folders and commands must be fullpath to run assuming
# the current folder is at the root of the git enlistment
$StartDate = (Get-Date)
$StartDate


$DebugPreference = "Continue"

# These tests are not google test suites and we should guard
# Against running them as suites
$RunOnly = New-Object System.Collections.Generic.HashSet[string]
$RunOnly.Add("c_test") | Out-Null
$RunOnly.Add("compact_on_deletion_collector_test") | Out-Null
$RunOnly.Add("merge_test") | Out-Null
$RunOnly.Add("stringappend_test") | Out-Null # Apparently incorrectly written
$RunOnly.Add("backupable_db_test") | Out-Null # Disabled
$RunOnly.Add("timer_queue_test") | Out-Null # Not a gtest

if($RunAll -and $SuiteRun -ne "") {
    Write-Error "$RunAll and $SuiteRun are not compatible"
    exit 1
}

if($RunAllExe -and $Run -ne "") {
    Write-Error "$RunAllExe and $Run are not compatible"
    exit 1
}

# If running under Appveyor assume that root
[string]$Appveyor = $Env:APPVEYOR_BUILD_FOLDER
if($Appveyor -ne "") {
    $RootFolder = $Appveyor
} else {
    $RootFolder = $PSScriptRoot -replace '\\build_tools', ''
}

$LogFolder = -Join($RootFolder, "\db_logs\")
$BinariesFolder = -Join($RootFolder, "\build\Debug\")

if($WorkFolder -eq "") {

    # If TEST_TMPDIR is set use it    
    [string]$var = $Env:TEST_TMPDIR
    if($var -eq "") {
        $WorkFolder = -Join($RootFolder, "\db_tests\")
        $Env:TEST_TMPDIR = $WorkFolder
    } else {
        $WorkFolder = $var
    }
} else {
# Override from a command line
  $Env:TEST_TMPDIR = $WorkFolder
}

Write-Output "Root: $RootFolder, WorkFolder: $WorkFolder"
Write-Output "BinariesFolder: $BinariesFolder, LogFolder: $LogFolder"

# Create test directories in the current folder
md -Path $WorkFolder -ErrorAction Ignore | Out-Null
md -Path $LogFolder -ErrorAction Ignore | Out-Null


$ExcludeCasesSet = New-Object System.Collections.Generic.HashSet[string]
if($ExcludeCases -ne "") {
    Write-Host "ExcludeCases: $ExcludeCases"
    $l = $ExcludeCases -split ' '
    ForEach($t in $l) { 
      $ExcludeCasesSet.Add($t) | Out-Null
    }
}

$ExcludeExesSet = New-Object System.Collections.Generic.HashSet[string]
if($ExcludeExes -ne "") {
    Write-Host "ExcludeExe: $ExcludeExes"
    $l = $ExcludeExes -split ' '
    ForEach($t in $l) { 
      $ExcludeExesSet.Add($t) | Out-Null
    }
}


# Extract the names of its tests by running db_test with --gtest_list_tests.
# This filter removes the "#"-introduced comments, and expands to
# fully-qualified names by changing input like this:
#
#   DBTest.
#     Empty
#     WriteEmptyBatch
#   MultiThreaded/MultiThreadedDBTest.
#     MultiThreaded/0  # GetParam() = 0
#     MultiThreaded/1  # GetParam() = 1
#
# into this:
#
#   DBTest.Empty
#   DBTest.WriteEmptyBatch
#   MultiThreaded/MultiThreadedDBTest.MultiThreaded/0
#   MultiThreaded/MultiThreadedDBTest.MultiThreaded/1
#
# Output into the parameter in a form TestName -> Log File Name
function ExtractTestCases([string]$GTestExe, $HashTable) {

    $Tests = @()
# Run db_test to get a list of tests and store it into $a array
    &$GTestExe --gtest_list_tests | tee -Variable Tests | Out-Null

    # Current group
    $Group=""

    ForEach( $l in $Tests) {

      # Leading whitespace is fine
      $l = $l -replace '^\s+',''
      # Trailing dot is a test group but no whitespace
      if ($l -match "\.$" -and $l -notmatch "\s+") {
        $Group = $l
      }  else {
        # Otherwise it is a test name, remove leading space
        $test = $l
        # remove trailing comment if any and create a log name
        $test = $test -replace '\s+\#.*',''
        $test = "$Group$test"

        if($ExcludeCasesSet.Contains($test)) {
            Write-Warning "$test case is excluded"
            continue
        }

        $test_log = $test -replace '[\./]','_'
        $test_log += ".log"
        $log_path = -join ($LogFolder, $test_log)

        # Add to a hashtable
        $HashTable.Add($test, $log_path);
      }
    }
}

# The function removes trailing .exe siffix if any,
# creates a name for the log file
# Then adds the test name if it was not excluded into
# a HashTable in a form of test_name -> log_path
function MakeAndAdd([string]$token, $HashTable) {

    $test_name = $token -replace '.exe$', ''
    $log_name =  -join ($test_name, ".log")
    $log_path = -join ($LogFolder, $log_name)
    $HashTable.Add($test_name, $log_path)
}

# This function takes a list of Suites to run
# Lists all the test cases in each of the suite
# and populates HashOfHashes
# Ordered by suite(exe) @{ Exe = @{ TestCase = LogName }}
function ProcessSuites($ListOfSuites, $HashOfHashes) {

  $suite_list = $ListOfSuites
  # Problem: if you run --gtest_list_tests on
  # a non Google Test executable then it will start executing
  # and we will get nowhere
  ForEach($suite in $suite_list) {

    if($RunOnly.Contains($suite)) {
      Write-Warning "$suite is excluded from running as Google test suite"
      continue
    }

    if($EnableJE) {
      $suite += "_je"
    }

    $Cases = [ordered]@{}
    $Cases.Clear()
    $suite_exe = -Join ($BinariesFolder, $suite)
    ExtractTestCases -GTestExe $suite_exe -HashTable $Cases
    if($Cases.Count -gt 0) {
      $HashOfHashes.Add($suite, $Cases);
    }
  }

  # Make logs and run
  if($CasesToRun.Count -lt 1) {
     Write-Error "Failed to extract tests from $SuiteRun"
     exit 1
  }

}

# This will contain all test executables to run

# Hash table that contains all non suite
# Test executable to run
$TestExes = [ordered]@{}

# Check for test exe that are not
# Google Test Suites
# Since this is explicitely mentioned it is not subject
# for exclusions
if($Run -ne "") {

  $test_list = $Run -split ' '
  ForEach($t in $test_list) {

    if($EnableJE) {
      $t += "_je"
    }
    MakeAndAdd -token $t -HashTable $TestExes
  }

  if($TestExes.Count -lt 1) {
     Write-Error "Failed to extract tests from $Run"
     exit 1
  }
} elseif($RunAllExe) {
  # Discover all the test binaries
  if($EnableJE) {
    $pattern = "*_test_je.exe"
  } else {
    $pattern = "*_test.exe"
  }

  $search_path = -join ($BinariesFolder, $pattern)
  Write-Host "Binaries Search Path: $search_path"

  $DiscoveredExe = @()
  dir -Path $search_path | ForEach-Object {
     $DiscoveredExe += ($_.Name)     
  }

  # Remove exclusions
  ForEach($e in $DiscoveredExe) {
    $e = $e -replace '.exe$', ''
    $bare_name = $e -replace '_je$', ''

    if($ExcludeExesSet.Contains($bare_name)) {
      Write-Warning "Test $e is excluded"
      continue
    }
    MakeAndAdd -token $e -HashTable $TestExes
  }

  if($TestExes.Count -lt 1) {
     Write-Error "Failed to discover test executables"
     exit 1
  }
}

# Ordered by exe @{ Exe = @{ TestCase = LogName }}
$CasesToRun = [ordered]@{}

if($SuiteRun -ne "") {
  $suite_list = $SuiteRun -split ' '
  ProcessSuites -ListOfSuites $suite_list -HashOfHashes $CasesToRun
} elseif ($RunAll) {
# Discover all the test binaries
  if($EnableJE) {
    $pattern = "*_test_je.exe"
  } else {
    $pattern = "*_test.exe"
  }

  $search_path = -join ($BinariesFolder, $pattern)
  Write-Host "Binaries Search Path: $search_path"

  $ListOfExe = @()
  dir -Path $search_path | ForEach-Object {
     $ListOfExe += ($_.Name)     
  }

  # Exclude those in RunOnly from running as suites
  $ListOfSuites = @()
  ForEach($e in $ListOfExe) {

    $e = $e -replace '.exe$', ''
    $bare_name = $e -replace '_je$', ''

    if($ExcludeExesSet.Contains($bare_name)) {
      Write-Warning "Test $e is excluded"
      continue
    }

    if($RunOnly.Contains($bare_name)) {
      MakeAndAdd -token $e -HashTable $TestExes
    } else {
      $ListOfSuites += $bare_name
    }
  }

  ProcessSuites -ListOfSuites $ListOfSuites -HashOfHashes $CasesToRun
}


# Invoke a test with a filter and redirect all output
$InvokeTestCase = {
    param($exe, $test, $log);
    &$exe --gtest_filter=$test > $log 2>&1
}

# Invoke all tests and redirect output
$InvokeTestAsync = {
    param($exe, $log)
    &$exe > $log 2>&1
}

# Hash that contains tests to rerun if any failed
# Those tests will be rerun sequentially
# $Rerun = [ordered]@{}
# Test limiting factor here
[int]$count = 0
# Overall status
[bool]$script:success = $true;

function RunJobs($Suites, $TestCmds, [int]$ConcurrencyVal)
{
    # Array to wait for any of the running jobs
    $jobs = @()
    # Hash JobToLog
    $JobToLog = @{}

    # Wait for all to finish and get the results
    while(($JobToLog.Count -gt 0) -or
          ($TestCmds.Count -gt 0) -or 
           ($Suites.Count -gt 0)) {

        # Make sure we have maximum concurrent jobs running if anything
        # and the $Limit either not set or allows to proceed
        while(($JobToLog.Count -lt $ConcurrencyVal) -and
              ((($TestCmds.Count -gt 0) -or ($Suites.Count -gt 0)) -and
              (($Limit -lt 0) -or ($count -lt $Limit)))) {

            # We always favore suites to run if available
            [string]$exe_name = ""
            [string]$log_path = ""
            $Cases = @{}

            if($Suites.Count -gt 0) {
              # Will the first one
              ForEach($e in $Suites.Keys) {
                $exe_name = $e
                $Cases = $Suites[$e]
                break
              }
              [string]$test_case = ""
              [string]$log_path = ""
              ForEach($c in $Cases.Keys) {
                 $test_case = $c
                 $log_path = $Cases[$c]
                 break
              }

              Write-Host "Starting $exe_name::$test_case"
              [string]$Exe =  -Join ($BinariesFolder, $exe_name)
              $job = Start-Job -Name "$exe_name::$test_case" -ArgumentList @($Exe,$test_case,$log_path) -ScriptBlock $InvokeTestCase
              $JobToLog.Add($job, $log_path)

              $Cases.Remove($test_case)
              if($Cases.Count -lt 1) {
                $Suites.Remove($exe_name)
              }

            } elseif ($TestCmds.Count -gt 0) {

               ForEach($e in $TestCmds.Keys) {
                 $exe_name = $e
                 $log_path = $TestCmds[$e]
                 break
               }

              Write-Host "Starting $exe_name"
              [string]$Exe =  -Join ($BinariesFolder, $exe_name)
              $job = Start-Job -Name $exe_name -ScriptBlock $InvokeTestAsync -ArgumentList @($Exe,$log_path)
              $JobToLog.Add($job, $log_path)

              $TestCmds.Remove($exe_name)

            } else {
                Write-Error "In the job loop but nothing to run"
                exit 1
            }

            ++$count
        } # End of Job starting loop

        if($JobToLog.Count -lt 1) {
          break
        }

        $jobs = @()
        foreach($k in $JobToLog.Keys) { $jobs += $k }

        $completed = Wait-Job -Job $jobs -Any
        $log = $JobToLog[$completed]
        $JobToLog.Remove($completed)

        $message = -join @($completed.Name, " State: ", ($completed.State))

        $log_content = @(Get-Content $log)

        if($completed.State -ne "Completed") {
            $script:success = $false
            Write-Warning $message
            $log_content | Write-Warning
        } else {
            # Scan the log. If we find PASSED and no occurrence of FAILED
            # then it is a success
            [bool]$pass_found = $false
            ForEach($l in $log_content) {

                if(($l -match "^\[\s+FAILED") -or
                   ($l -match "Assertion failed:")) {
                    $pass_found = $false
                    break
                }

                if(($l -match "^\[\s+PASSED") -or
                   ($l -match " : PASSED$") -or
                    ($l -match "^PASS$") -or   # Special c_test case
                    ($l -match "Passed all tests!") ) {
                    $pass_found = $true
                }
            }

            if(!$pass_found) {
                $script:success = $false;
                Write-Warning $message
                $log_content | Write-Warning
            } else {
                Write-Host $message
            }
        }

        # Remove cached job info from the system
        # Should be no output
        Receive-Job -Job $completed | Out-Null
    }
}

RunJobs -Suites $CasesToRun -TestCmds $TestExes -ConcurrencyVal $Concurrency

$EndDate = (Get-Date)

New-TimeSpan -Start $StartDate -End $EndDate | 
  ForEach-Object { 
    "Elapsed time: {0:g}" -f $_
  }


if(!$script:success) {
# This does not succeed killing off jobs quick
# So we simply exit
#    Remove-Job -Job $jobs -Force
# indicate failure using this exit code
    exit 1
 }

 exit 0

 
