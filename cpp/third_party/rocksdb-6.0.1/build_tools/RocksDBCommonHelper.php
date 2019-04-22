<?php
// Copyright 2004-present Facebook. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Name of the environment variables which need to be set by the entity which
// triggers continuous runs so that code at the end of the file gets executed
// and Sandcastle run starts.
const ENV_POST_RECEIVE_HOOK = "POST_RECEIVE_HOOK";
const ENV_HTTPS_APP_VALUE = "HTTPS_APP_VALUE";
const ENV_HTTPS_TOKEN_VALUE = "HTTPS_TOKEN_VALUE";

const PRIMARY_TOKEN_FILE = '/home/krad/.sandcastle';
const CONT_RUN_ALIAS = "leveldb";

//////////////////////////////////////////////////////////////////////
/*  Run tests in sandcastle */
function postURL($diffID, $url) {
  assert(strlen($diffID) > 0);
  assert(is_numeric($diffID));
  assert(strlen($url) > 0);

  $cmd_args = array(
    'diff_id' => (int)$diffID,
    'name' => sprintf(
      'click here for sandcastle tests for D%d',
      (int)$diffID
    ),
    'link' => $url
  );
  $cmd = 'echo ' . escapeshellarg(json_encode($cmd_args))
         . ' | arc call-conduit differential.updateunitresults';

  shell_exec($cmd);
}

function buildUpdateTestStatusCmd($diffID, $test, $status) {
  assert(strlen($diffID) > 0);
  assert(is_numeric($diffID));
  assert(strlen($test) > 0);
  assert(strlen($status) > 0);

  $cmd_args = array(
    'diff_id' => (int)$diffID,
    'name' => $test,
    'result' => $status
  );

  $cmd = 'echo ' . escapeshellarg(json_encode($cmd_args))
         . ' | arc call-conduit differential.updateunitresults';

  return $cmd;
}

function updateTestStatus($diffID, $test) {
  assert(strlen($diffID) > 0);
  assert(is_numeric($diffID));
  assert(strlen($test) > 0);

  shell_exec(buildUpdateTestStatusCmd($diffID, $test, "waiting"));
}

function getSteps($applyDiff, $diffID, $username, $test) {
  assert(strlen($username) > 0);
  assert(strlen($test) > 0);

  if ($applyDiff) {
    assert(strlen($diffID) > 0);
    assert(is_numeric($diffID));

    $arcrc_content = (PHP_OS == "Darwin" ?
        exec("cat ~/.arcrc | gzip -f | base64") :
            exec("cat ~/.arcrc | gzip -f | base64 -w0"));
    assert(strlen($arcrc_content) > 0);

    // Sandcastle machines don't have arc setup. We copy the user certificate
    // and authenticate using that in Sandcastle.
    $setup = array(
      "name" => "Setup arcrc",
      "shell" => "echo " . escapeshellarg($arcrc_content) . " | base64 --decode"
                 . " | gzip -d > ~/.arcrc",
      "user" => "root"
    );

    // arc demands certain permission on its config.
    // also fix the sticky bit issue in sandcastle
    $fix_permission = array(
      "name" => "Fix environment",
      "shell" => "chmod 600 ~/.arcrc && chmod +t /dev/shm",
      "user" => "root"
    );

    // Construct the steps in the order of execution.
    $steps[] = $setup;
    $steps[] = $fix_permission;
  }

  // fbcode is a sub-repo. We cannot patch until we add it to ignore otherwise
  // Git thinks it is an uncommitted change.
  $fix_git_ignore = array(
    "name" => "Fix git ignore",
    "shell" => "echo fbcode >> .git/info/exclude",
    "user" => "root"
  );

  // This fixes "FATAL: ThreadSanitizer can not mmap the shadow memory"
  // Source:
  // https://github.com/google/sanitizers/wiki/ThreadSanitizerCppManual#FAQ
  $fix_kernel_issue = array(
    "name" => "Fix kernel issue with tsan",
    "shell" => "echo 2 >/proc/sys/kernel/randomize_va_space",
    "user" => "root"
  );

  $steps[] = $fix_git_ignore;
  $steps[] = $fix_kernel_issue;

  // This will be the command used to execute particular type of tests.
  $cmd = "";

  if ($applyDiff) {
    // Patch the code (keep your fingures crossed).
    $patch = array(
      "name" => "Patch " . $diffID,
      "shell" => "arc --arcrc-file ~/.arcrc "
                  . "patch --nocommit --diff " . escapeshellarg($diffID),
      "user" => "root"
    );

    $steps[] = $patch;

    updateTestStatus($diffID, $test);
    $cmd = buildUpdateTestStatusCmd($diffID, $test, "running") . "; ";
  }

  // Run the actual command.
  $cmd = $cmd . "J=$(nproc) ./build_tools/precommit_checker.py " .
           escapeshellarg($test) . "; exit_code=$?; ";

  if ($applyDiff) {
    $cmd = $cmd . "([[ \$exit_code -eq 0 ]] &&"
                . buildUpdateTestStatusCmd($diffID, $test, "pass") . ")"
                . "||" . buildUpdateTestStatusCmd($diffID, $test, "fail")
                . "; ";
  }

  // shell command to sort the tests based on exit code and print
  // the output of the log files.
  $cat_sorted_logs = "
    while read code log_file;
      do echo \"################ cat \$log_file [exit_code : \$code] ################\";
      cat \$log_file;
    done < <(tail -n +2 LOG | sort -k7,7n -k4,4gr | awk '{print \$7,\$NF}')";

  // Shell command to cat all log files
  $cat_all_logs = "for f in `ls t/!(run-*)`; do echo \$f;cat \$f; done";

  // If LOG file exist use it to cat log files sorted by exit code, otherwise
  // cat everything
  $logs_cmd = "if [ -f LOG ]; then {$cat_sorted_logs}; else {$cat_all_logs}; fi";

  $cmd = $cmd . " cat /tmp/precommit-check.log"
              . "; shopt -s extglob; {$logs_cmd}"
              . "; shopt -u extglob; [[ \$exit_code -eq 0 ]]";
  assert(strlen($cmd) > 0);

  $run_test = array(
    "name" => "Run " . $test,
    "shell" => $cmd,
    "user" => "root",
    "parser" => "python build_tools/error_filter.py " . escapeshellarg($test),
  );

  $steps[] = $run_test;

  if ($applyDiff) {
    // Clean up the user arc config we are using.
    $cleanup = array(
      "name" => "Arc cleanup",
      "shell" => "rm -f ~/.arcrc",
      "user" => "root"
    );

    $steps[] = $cleanup;
  }

  assert(count($steps) > 0);
  return $steps;
}

function getSandcastleConfig() {
  $sandcastle_config = array();

  $cwd = getcwd();
  $cwd_token_file = "{$cwd}/.sandcastle";
  // This is a case when we're executed from a continuous run. Fetch the values
  // from the environment.
  if (getenv(ENV_POST_RECEIVE_HOOK)) {
    $sandcastle_config[0] = getenv(ENV_HTTPS_APP_VALUE);
    $sandcastle_config[1] = getenv(ENV_HTTPS_TOKEN_VALUE);
  } else {
    // This is a typical `[p]arc diff` case. Fetch the values from the specific
    // configuration files.
    for ($i = 0; $i < 50; $i++) {
      if (file_exists(PRIMARY_TOKEN_FILE) ||
          file_exists($cwd_token_file)) {
        break;
      }
      // If we failed to fetch the tokens, sleep for 0.2 second and try again
      usleep(200000);
    }
    assert(file_exists(PRIMARY_TOKEN_FILE) ||
           file_exists($cwd_token_file));

    // Try the primary location first, followed by a secondary.
    if (file_exists(PRIMARY_TOKEN_FILE)) {
      $cmd = 'cat ' . PRIMARY_TOKEN_FILE;
    } else {
      $cmd = 'cat ' . escapeshellarg($cwd_token_file);
    }

    assert(strlen($cmd) > 0);
    $sandcastle_config = explode(':', rtrim(shell_exec($cmd)));
  }

  // In this case be very explicit about the implications.
  if (count($sandcastle_config) != 2) {
    echo "Sandcastle configuration files don't contain valid information " .
         "or the necessary environment variables aren't defined. Unable " .
         "to validate the code changes.";
    exit(1);
  }

  assert(strlen($sandcastle_config[0]) > 0);
  assert(strlen($sandcastle_config[1]) > 0);
  assert(count($sandcastle_config) > 0);

  return $sandcastle_config;
}

// This function can be called either from `[p]arc diff` command or during
// the Git post-receive hook.
 function startTestsInSandcastle($applyDiff, $workflow, $diffID) {
  // Default options don't terminate on failure, but that's what we want. In
  // the current case we use assertions intentionally as "terminate on failure
  // invariants".
  assert_options(ASSERT_BAIL, true);

  // In case of a diff we'll send notificatios to the author. Else it'll go to
  // the entire team because failures indicate that build quality has regressed.
  $username = $applyDiff ? exec("whoami") : CONT_RUN_ALIAS;
  assert(strlen($username) > 0);

  if ($applyDiff) {
    assert($workflow);
    assert(strlen($diffID) > 0);
    assert(is_numeric($diffID));
  }

  // List of tests we want to run in Sandcastle.
  $tests = array("unit", "unit_non_shm", "unit_481", "clang_unit", "tsan",
                 "asan", "lite_test", "valgrind", "release", "release_481",
                 "clang_release", "clang_analyze", "code_cov",
                 "java_build", "no_compression", "unity", "ubsan");

  $send_email_template = array(
    'type' => 'email',
    'triggers' => array('fail'),
    'emails' => array($username . '@fb.com'),
  );

  // Construct a job definition for each test and add it to the master plan.
  foreach ($tests as $test) {
    $stepName = "RocksDB diff " . $diffID . " test " . $test;

    if (!$applyDiff) {
      $stepName = "RocksDB continuous integration test " . $test;
    }

    $arg[] = array(
      "name" => $stepName,
      "report" => array($send_email_template),
      "steps" => getSteps($applyDiff, $diffID, $username, $test)
    );
  }

  // We cannot submit the parallel execution master plan to Sandcastle and
  // need supply the job plan as a determinator. So we construct a small job
  // that will spit out the master job plan which Sandcastle will parse and
  // execute. Why compress the job definitions? Otherwise we run over the max
  // string size.
  $cmd = "echo " . base64_encode(json_encode($arg))
         . (PHP_OS == "Darwin" ?
             " | gzip -f | base64" :
                 " | gzip -f | base64 -w0");
  assert(strlen($cmd) > 0);

  $arg_encoded = shell_exec($cmd);
  assert(strlen($arg_encoded) > 0);

  $runName = "Run diff " . $diffID . "for user " . $username;

  if (!$applyDiff) {
    $runName = "RocksDB continuous integration build and test run";
  }

  $command = array(
    "name" => $runName,
    "steps" => array()
  );

  $command["steps"][] = array(
    "name" => "Generate determinator",
    "shell" => "echo " . $arg_encoded . " | base64 --decode | gzip -d"
               . " | base64 --decode",
    "determinator" => true,
    "user" => "root"
  );

  // Submit to Sandcastle.
  $url = 'https://interngraph.intern.facebook.com/sandcastle/create';

  $job = array(
    'command' => 'SandcastleUniversalCommand',
    'args' => $command,
    'capabilities' => array(
      'vcs' => 'rocksdb-int-git',
      'type' => 'lego',
    ),
    'hash' => 'origin/master',
    'user' => $username,
    'alias' => 'rocksdb-precommit',
    'tags' => array('rocksdb'),
    'description' => 'Rocksdb precommit job',
  );

  // Fetch the configuration necessary to submit a successful HTTPS request.
  $sandcastle_config = getSandcastleConfig();

  $app = $sandcastle_config[0];
  $token = $sandcastle_config[1];

  $cmd = 'curl -s -k '
          . ' -F app=' . escapeshellarg($app)
          . ' -F token=' . escapeshellarg($token)
          . ' -F job=' . escapeshellarg(json_encode($job))
          .' ' . escapeshellarg($url);

  $output = shell_exec($cmd);
  assert(strlen($output) > 0);

  // Extract Sandcastle URL from the response.
  preg_match('/url": "(.+)"/', $output, $sandcastle_url);

  assert(count($sandcastle_url) > 0, "Unable to submit Sandcastle request.");
  assert(strlen($sandcastle_url[1]) > 0, "Unable to extract Sandcastle URL.");

  if ($applyDiff) {
    echo "\nSandcastle URL: " . $sandcastle_url[1] . "\n";
    // Ask Phabricator to display it on the diff UI.
    postURL($diffID, $sandcastle_url[1]);
  } else {
    echo "Continuous integration started Sandcastle tests. You can look at ";
    echo "the progress at:\n" . $sandcastle_url[1] . "\n";
  }
}

// Continuous run cript will set the environment variable and based on that
// we'll trigger the execution of tests in Sandcastle. In that case we don't
// need to apply any diffs and there's no associated workflow either.
if (getenv(ENV_POST_RECEIVE_HOOK)) {
  startTestsInSandcastle(
    false /* $applyDiff */,
    NULL /* $workflow */,
    NULL /* $diffID */);
}
