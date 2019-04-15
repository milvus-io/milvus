#!/usr/bin/env php
<?php
/*
   +----------------------------------------------------------------------+
   | PHP Version 7                                                        |
   +----------------------------------------------------------------------+
   | Copyright (c) 1997-2018 The PHP Group                                |
   +----------------------------------------------------------------------+
   | This source file is subject to version 3.01 of the PHP license,      |
   | that is bundled with this package in the file LICENSE, and is        |
   | available through the world-wide-web at the following url:           |
   | http://www.php.net/license/3_01.txt                                  |
   | If you did not receive a copy of the PHP license and are unable to   |
   | obtain it through the world-wide-web, please send a note to          |
   | license@php.net so we can mail you a copy immediately.               |
   +----------------------------------------------------------------------+
   | Authors: Ilia Alshanetsky <iliaa@php.net>                            |
   |          Preston L. Bannister <pbannister@php.net>                   |
   |          Marcus Boerger <helly@php.net>                              |
   |          Derick Rethans <derick@php.net>                             |
   |          Sander Roobol <sander@php.net>                              |
   | (based on version by: Stig Bakken <ssb@php.net>)                     |
   | (based on the PHP 3 test framework by Rasmus Lerdorf)                |
   +----------------------------------------------------------------------+
 */

/* $Id: f4720f5b76a811a5f7ee1b19ce15e4c68ecf7307 $ */

/* Sanity check to ensure that pcre extension needed by this script is available.
 * In the event it is not, print a nice error message indicating that this script will
 * not run without it.
 */

if (!extension_loaded('pcre')) {
	echo <<<NO_PCRE_ERROR

+-----------------------------------------------------------+
|                       ! ERROR !                           |
| The test-suite requires that you have pcre extension      |
| enabled. To enable this extension either compile your PHP |
| with --with-pcre-regex or if you've compiled pcre as a    |
| shared module load it via php.ini.                        |
+-----------------------------------------------------------+

NO_PCRE_ERROR;
exit(1);
}

if (!function_exists('proc_open')) {
	echo <<<NO_PROC_OPEN_ERROR

+-----------------------------------------------------------+
|                       ! ERROR !                           |
| The test-suite requires that proc_open() is available.    |
| Please check if you disabled it in php.ini.               |
+-----------------------------------------------------------+

NO_PROC_OPEN_ERROR;
exit(1);
}

// If timezone is not set, use UTC.
if (ini_get('date.timezone') == '') {
	date_default_timezone_set('UTC');
}

// store current directory
$CUR_DIR = getcwd();

// change into the PHP source directory.

if (getenv('TEST_PHP_SRCDIR')) {
	@chdir(getenv('TEST_PHP_SRCDIR'));
}

// Delete some security related environment variables
putenv('SSH_CLIENT=deleted');
putenv('SSH_AUTH_SOCK=deleted');
putenv('SSH_TTY=deleted');
putenv('SSH_CONNECTION=deleted');

$cwd = getcwd();
set_time_limit(0);

ini_set('pcre.backtrack_limit', PHP_INT_MAX);

$valgrind_version = 0;
$valgrind_header = '';

// delete as much output buffers as possible
while(@ob_end_clean());
if (ob_get_level()) echo "Not all buffers were deleted.\n";

error_reporting(E_ALL);

$environment = isset($_ENV) ? $_ENV : array();
// Note: php.ini-development sets variables_order="GPCS" not "EGPCS", in which case $_ENV is NOT populated.
//       detect and handle this case, or die or warn
if (empty($environment)) {
	// not documented, but returns array of all environment variables
	$environment = getenv();
}
if (empty($environment['TEMP'])) {
	$environment['TEMP'] = sys_get_temp_dir();
	
	if (empty($environment['TEMP'])) {
		// for example, OpCache on Windows will fail in this case because child processes (for tests) will not get
		// a TEMP variable, so GetTempPath() will fallback to c:\windows, while GetTempPath() will return %TEMP% for parent
		// (likely a different path). The parent will initialize the OpCache in that path, and child will fail to reattach to
		// the OpCache because it will be using the wrong path.
		die("TEMP environment is NOT set");
	} else if (count($environment)==1) {
		// not having other environment variables, only having TEMP, is probably ok, but strange and may make a 
		// difference in the test pass rate, so warn the user.
		echo "WARNING: Only 1 environment variable will be available to tests(TEMP environment variable)".PHP_EOL;
	}
}
//
if ((substr(PHP_OS, 0, 3) == "WIN") && empty($environment["SystemRoot"])) {
  $environment["SystemRoot"] = getenv("SystemRoot");
}

// Don't ever guess at the PHP executable location.
// Require the explicit specification.
// Otherwise we could end up testing the wrong file!

$php = null;
$php_cgi = null;
$phpdbg = null;

if (getenv('TEST_PHP_EXECUTABLE')) {
	$php = getenv('TEST_PHP_EXECUTABLE');

	if ($php=='auto') {
		$php = $cwd . '/sapi/cli/php';
		putenv("TEST_PHP_EXECUTABLE=$php");

		if (!getenv('TEST_PHP_CGI_EXECUTABLE')) {
			$php_cgi = $cwd . '/sapi/cgi/php-cgi';

			if (file_exists($php_cgi)) {
				putenv("TEST_PHP_CGI_EXECUTABLE=$php_cgi");
			} else {
				$php_cgi = null;
			}
		}
	}
	$environment['TEST_PHP_EXECUTABLE'] = $php;
}

if (getenv('TEST_PHP_CGI_EXECUTABLE')) {
	$php_cgi = getenv('TEST_PHP_CGI_EXECUTABLE');

	if ($php_cgi=='auto') {
		$php_cgi = $cwd . '/sapi/cgi/php-cgi';
		putenv("TEST_PHP_CGI_EXECUTABLE=$php_cgi");
	}

	$environment['TEST_PHP_CGI_EXECUTABLE'] = $php_cgi;
}

if (!getenv('TEST_PHPDBG_EXECUTABLE')) {
	if (!strncasecmp(PHP_OS, "win", 3) && file_exists(dirname($php) . "/phpdbg.exe")) {
		$phpdbg = realpath(dirname($php) . "/phpdbg.exe");
	} elseif (file_exists(dirname($php) . "/../../sapi/phpdbg/phpdbg")) {
		$phpdbg = realpath(dirname($php) . "/../../sapi/phpdbg/phpdbg");
	} elseif (file_exists("./sapi/phpdbg/phpdbg")) {
		$phpdbg = realpath("./sapi/phpdbg/phpdbg");
	} elseif (file_exists(dirname($php) . "/phpdbg")) {
		$phpdbg = realpath(dirname($php) . "/phpdbg");
	} else {
		$phpdbg = null;
	}
	if ($phpdbg) {
		putenv("TEST_PHPDBG_EXECUTABLE=$phpdbg");
	}
}

if (getenv('TEST_PHPDBG_EXECUTABLE')) {
	$phpdbg = getenv('TEST_PHPDBG_EXECUTABLE');

	if ($phpdbg=='auto') {
		$phpdbg = $cwd . '/sapi/phpdbg/phpdbg';
		putenv("TEST_PHPDBG_EXECUTABLE=$phpdbg");
	}

	$environment['TEST_PHPDBG_EXECUTABLE'] = $phpdbg;
}

function verify_config()
{
	global $php;

	if (empty($php) || !file_exists($php)) {
		error('environment variable TEST_PHP_EXECUTABLE must be set to specify PHP executable!');
	}

	if (function_exists('is_executable') && !is_executable($php)) {
		error("invalid PHP executable specified by TEST_PHP_EXECUTABLE  = $php");
	}
}

if (getenv('TEST_PHP_LOG_FORMAT')) {
	$log_format = strtoupper(getenv('TEST_PHP_LOG_FORMAT'));
} else {
	$log_format = 'LEODS';
}

// Check whether a detailed log is wanted.
if (getenv('TEST_PHP_DETAILED')) {
	$DETAILED = getenv('TEST_PHP_DETAILED');
} else {
	$DETAILED = 0;
}

junit_init();

if (getenv('SHOW_ONLY_GROUPS')) {
	$SHOW_ONLY_GROUPS = explode(",", getenv('SHOW_ONLY_GROUPS'));
} else {
	$SHOW_ONLY_GROUPS = array();
}

// Check whether user test dirs are requested.
if (getenv('TEST_PHP_USER')) {
	$user_tests = explode (',', getenv('TEST_PHP_USER'));
} else {
	$user_tests = array();
}

$exts_to_test = array();
$ini_overwrites = array(
		'output_handler=',
		'open_basedir=',
		'disable_functions=',
		'output_buffering=Off',
		'error_reporting=' . (E_ALL | E_STRICT),
		'display_errors=1',
		'display_startup_errors=1',
		'log_errors=0',
		'html_errors=0',
		'track_errors=0',
		'report_memleaks=1',
		'report_zend_debug=0',
		'docref_root=',
		'docref_ext=.html',
		'error_prepend_string=',
		'error_append_string=',
		'auto_prepend_file=',
		'auto_append_file=',
		'ignore_repeated_errors=0',
		'precision=14',
		'memory_limit=128M',
		'log_errors_max_len=0',
		'opcache.fast_shutdown=0',
		'opcache.file_update_protection=0',
		'zend.assertions=1',
	);

$no_file_cache = '-d opcache.file_cache= -d opcache.file_cache_only=0';

function write_information()
{
	global $cwd, $php, $php_cgi, $phpdbg, $php_info, $user_tests, $ini_overwrites, $pass_options, $exts_to_test, $leak_check, $valgrind_header, $no_file_cache;

	// Get info from php
	$info_file = __DIR__ . '/run-test-info.php';
	@unlink($info_file);
	$php_info = '<?php echo "
PHP_SAPI    : " , PHP_SAPI , "
PHP_VERSION : " , phpversion() , "
ZEND_VERSION: " , zend_version() , "
PHP_OS      : " , PHP_OS , " - " , php_uname() , "
INI actual  : " , realpath(get_cfg_var("cfg_file_path")) , "
More .INIs  : " , (function_exists(\'php_ini_scanned_files\') ? str_replace("\n","", php_ini_scanned_files()) : "** not determined **"); ?>';
	save_text($info_file, $php_info);
	$info_params = array();
	settings2array($ini_overwrites, $info_params);
	settings2params($info_params);
	$php_info = `$php $pass_options $info_params $no_file_cache "$info_file"`;
	define('TESTED_PHP_VERSION', `$php -n -r "echo PHP_VERSION;"`);

	if ($php_cgi && $php != $php_cgi) {
		$php_info_cgi = `$php_cgi $pass_options $info_params $no_file_cache -q "$info_file"`;
		$php_info_sep = "\n---------------------------------------------------------------------";
		$php_cgi_info = "$php_info_sep\nPHP         : $php_cgi $php_info_cgi$php_info_sep";
	} else {
		$php_cgi_info = '';
	}

	if ($phpdbg) {
		$phpdbg_info = `$phpdbg $pass_options $info_params $no_file_cache -qrr "$info_file"`;
		$php_info_sep = "\n---------------------------------------------------------------------";
		$phpdbg_info = "$php_info_sep\nPHP         : $phpdbg $phpdbg_info$php_info_sep";
	} else {
		$phpdbg_info = '';
	}

	if (function_exists('opcache_invalidate')) {
		opcache_invalidate($info_file, true);
	}
	@unlink($info_file);

	// load list of enabled extensions
	save_text($info_file, '<?php echo str_replace("Zend OPcache", "opcache", implode(",", get_loaded_extensions())); ?>');
	$exts_to_test = explode(',',`$php $pass_options $info_params $no_file_cache "$info_file"`);
	// check for extensions that need special handling and regenerate
	$info_params_ex = array(
		'session' => array('session.auto_start=0'),
		'tidy' => array('tidy.clean_output=0'),
		'zlib' => array('zlib.output_compression=Off'),
		'xdebug' => array('xdebug.default_enable=0'),
		'mbstring' => array('mbstring.func_overload=0'),
	);

	foreach($info_params_ex as $ext => $ini_overwrites_ex) {
		if (in_array($ext, $exts_to_test)) {
			$ini_overwrites = array_merge($ini_overwrites, $ini_overwrites_ex);
		}
	}

	if (function_exists('opcache_invalidate')) {
		opcache_invalidate($info_file, true);
	}
	@unlink($info_file);

	// Write test context information.
	echo "
=====================================================================
PHP         : $php $php_info $php_cgi_info $phpdbg_info
CWD         : $cwd
Extra dirs  : ";
	foreach ($user_tests as $test_dir) {
		echo "{$test_dir}\n              ";
	}
	echo "
VALGRIND    : " . ($leak_check ? $valgrind_header : 'Not used') . "
=====================================================================
";
}

define('PHP_QA_EMAIL', 'qa-reports@lists.php.net');
define('QA_SUBMISSION_PAGE', 'http://qa.php.net/buildtest-process.php');
define('QA_REPORTS_PAGE', 'http://qa.php.net/reports');
define('TRAVIS_CI' , (bool) getenv('TRAVIS'));

function save_or_mail_results()
{
	global $sum_results, $just_save_results, $failed_test_summary,
		   $PHP_FAILED_TESTS, $CUR_DIR, $php, $output_file;

	/* We got failed Tests, offer the user to send an e-mail to QA team, unless NO_INTERACTION is set */
	if (!getenv('NO_INTERACTION') && !TRAVIS_CI) {
		$fp = fopen("php://stdin", "r+");
		if ($sum_results['FAILED'] || $sum_results['BORKED'] || $sum_results['WARNED'] || $sum_results['LEAKED']) {
			echo "\nYou may have found a problem in PHP.";
		}
		echo "\nThis report can be automatically sent to the PHP QA team at\n";
		echo QA_REPORTS_PAGE . " and http://news.php.net/php.qa.reports\n";
		echo "This gives us a better understanding of PHP's behavior.\n";
		echo "If you don't want to send the report immediately you can choose\n";
		echo "option \"s\" to save it.	You can then email it to ". PHP_QA_EMAIL . " later.\n";
		echo "Do you want to send this report now? [Yns]: ";
		flush();

		$user_input = fgets($fp, 10);
		$just_save_results = (strtolower($user_input[0]) == 's');
	}

	if ($just_save_results || !getenv('NO_INTERACTION') || TRAVIS_CI) {
		if ($just_save_results || TRAVIS_CI || strlen(trim($user_input)) == 0 || strtolower($user_input[0]) == 'y') {
			/*
			 * Collect information about the host system for our report
			 * Fetch phpinfo() output so that we can see the PHP environment
			 * Make an archive of all the failed tests
			 * Send an email
			 */
			if ($just_save_results) {
				$user_input = 's';
			}

			/* Ask the user to provide an email address, so that QA team can contact the user */
			if (TRAVIS_CI) {
				$user_email = 'travis at php dot net';
			} elseif (!strncasecmp($user_input, 'y', 1) || strlen(trim($user_input)) == 0) {
				echo "\nPlease enter your email address.\n(Your address will be mangled so that it will not go out on any\nmailinglist in plain text): ";
				flush();
				$user_email = trim(fgets($fp, 1024));
				$user_email = str_replace("@", " at ", str_replace(".", " dot ", $user_email));
			}

			$failed_tests_data = '';
			$sep = "\n" . str_repeat('=', 80) . "\n";
			$failed_tests_data .= $failed_test_summary . "\n";
			$failed_tests_data .= get_summary(true, false) . "\n";

			if ($sum_results['FAILED']) {
				foreach ($PHP_FAILED_TESTS['FAILED'] as $test_info) {
					$failed_tests_data .= $sep . $test_info['name'] . $test_info['info'];
					$failed_tests_data .= $sep . file_get_contents(realpath($test_info['output']), FILE_BINARY);
					$failed_tests_data .= $sep . file_get_contents(realpath($test_info['diff']), FILE_BINARY);
					$failed_tests_data .= $sep . "\n\n";
				}
				$status = "failed";
			} else {
				$status = "success";
			}

			$failed_tests_data .= "\n" . $sep . 'BUILD ENVIRONMENT' . $sep;
			$failed_tests_data .= "OS:\n" . PHP_OS . " - " . php_uname() . "\n\n";
			$ldd = $autoconf = $sys_libtool = $libtool = $compiler = 'N/A';

			if (substr(PHP_OS, 0, 3) != "WIN") {
				/* If PHP_AUTOCONF is set, use it; otherwise, use 'autoconf'. */
				if (getenv('PHP_AUTOCONF')) {
					$autoconf = shell_exec(getenv('PHP_AUTOCONF') . ' --version');
				} else {
					$autoconf = shell_exec('autoconf --version');
				}

				/* Always use the generated libtool - Mac OSX uses 'glibtool' */
				$libtool = shell_exec($CUR_DIR . '/libtool --version');

				/* Use shtool to find out if there is glibtool present (MacOSX) */
				$sys_libtool_path = shell_exec(__DIR__ . '/build/shtool path glibtool libtool');

				if ($sys_libtool_path) {
					$sys_libtool = shell_exec(str_replace("\n", "", $sys_libtool_path) . ' --version');
				}

				/* Try the most common flags for 'version' */
				$flags = array('-v', '-V', '--version');
				$cc_status = 0;

				foreach($flags AS $flag) {
					system(getenv('CC') . " $flag >/dev/null 2>&1", $cc_status);
					if ($cc_status == 0) {
						$compiler = shell_exec(getenv('CC') . " $flag 2>&1");
						break;
					}
				}

				$ldd = shell_exec("ldd $php 2>/dev/null");
			}

			$failed_tests_data .= "Autoconf:\n$autoconf\n";
			$failed_tests_data .= "Bundled Libtool:\n$libtool\n";
			$failed_tests_data .= "System Libtool:\n$sys_libtool\n";
			$failed_tests_data .= "Compiler:\n$compiler\n";
			$failed_tests_data .= "Bison:\n". shell_exec('bison --version 2>/dev/null') . "\n";
			$failed_tests_data .= "Libraries:\n$ldd\n";
			$failed_tests_data .= "\n";

			if (isset($user_email)) {
				$failed_tests_data .= "User's E-mail: " . $user_email . "\n\n";
			}

			$failed_tests_data .= $sep . "PHPINFO" . $sep;
			$failed_tests_data .= shell_exec($php . ' -ddisplay_errors=stderr -dhtml_errors=0 -i 2> /dev/null');

			if (($just_save_results || !mail_qa_team($failed_tests_data, $status)) && !TRAVIS_CI) {
				file_put_contents($output_file, $failed_tests_data);

				if (!$just_save_results) {
					echo "\nThe test script was unable to automatically send the report to PHP's QA Team\n";
				}

				echo "Please send " . $output_file . " to " . PHP_QA_EMAIL . " manually, thank you.\n";
			} elseif (!getenv('NO_INTERACTION') && !TRAVIS_CI) {
				fwrite($fp, "\nThank you for helping to make PHP better.\n");
				fclose($fp);
			}
		}
	}
}

// Determine the tests to be run.

$test_files = array();
$redir_tests = array();
$test_results = array();
$PHP_FAILED_TESTS = array('BORKED' => array(), 'FAILED' => array(), 'WARNED' => array(), 'LEAKED' => array(), 'XFAILED' => array(), 'SLOW' => array());

// If parameters given assume they represent selected tests to run.
$result_tests_file= false;
$failed_tests_file= false;
$pass_option_n = false;
$pass_options = '';

$output_file = $CUR_DIR . '/php_test_results_' . date('Ymd_Hi') . '.txt';

$just_save_results = false;
$leak_check = false;
$html_output = false;
$html_file = null;
$temp_source = null;
$temp_target = null;
$temp_urlbase = null;
$conf_passed = null;
$no_clean = false;
$slow_min_ms = INF;

$cfgtypes = array('show', 'keep');
$cfgfiles = array('skip', 'php', 'clean', 'out', 'diff', 'exp');
$cfg = array();

foreach($cfgtypes as $type) {
	$cfg[$type] = array();

	foreach($cfgfiles as $file) {
		$cfg[$type][$file] = false;
	}
}

if (getenv('TEST_PHP_ARGS')) {

	if (!isset($argc, $argv) || !$argc) {
		$argv = array(__FILE__);
	}

	$argv = array_merge($argv, explode(' ', getenv('TEST_PHP_ARGS')));
	$argc = count($argv);
}

if (isset($argc) && $argc > 1) {

	for ($i=1; $i<$argc; $i++) {
		$is_switch = false;
		$switch = substr($argv[$i],1,1);
		$repeat = substr($argv[$i],0,1) == '-';

		while ($repeat) {

			if (!$is_switch) {
				$switch = substr($argv[$i],1,1);
			}

			$is_switch = true;

			if ($repeat) {
				foreach($cfgtypes as $type) {
					if (strpos($switch, '--' . $type) === 0) {
						foreach($cfgfiles as $file) {
							if ($switch == '--' . $type . '-' . $file) {
								$cfg[$type][$file] = true;
								$is_switch = false;
								break;
							}
						}
					}
				}
			}

			if (!$is_switch) {
				$is_switch = true;
				break;
			}

			$repeat = false;

			switch($switch) {
				case 'r':
				case 'l':
					$test_list = file($argv[++$i]);
					if ($test_list) {
						foreach($test_list as $test) {
							$matches = array();
							if (preg_match('/^#.*\[(.*)\]\:\s+(.*)$/', $test, $matches)) {
								$redir_tests[] = array($matches[1], $matches[2]);
							} else if (strlen($test)) {
								$test_files[] = trim($test);
							}
						}
					}
					if ($switch != 'l') {
						break;
					}
					$i--;
					// break left intentionally
				case 'w':
					$failed_tests_file = fopen($argv[++$i], 'w+t');
					break;
				case 'a':
					$failed_tests_file = fopen($argv[++$i], 'a+t');
					break;
				case 'W':
					$result_tests_file = fopen($argv[++$i], 'w+t');
					break;
				case 'c':
					$conf_passed = $argv[++$i];
					break;
				case 'd':
					$ini_overwrites[] = $argv[++$i];
					break;
				case 'g':
					$SHOW_ONLY_GROUPS = explode(",", $argv[++$i]);
					break;
				//case 'h'
				case '--keep-all':
					foreach($cfgfiles as $file) {
						$cfg['keep'][$file] = true;
					}
					break;
				//case 'l'
				case 'm':
					$leak_check = true;
					$valgrind_cmd = "valgrind --version";
					$valgrind_header = system_with_timeout($valgrind_cmd, $environment);
					$replace_count = 0;
					if (!$valgrind_header) {
						error("Valgrind returned no version info, cannot proceed.\nPlease check if Valgrind is installed.");
					} else {
						$valgrind_version = preg_replace("/valgrind-(\d+)\.(\d+)\.(\d+)([.\w_-]+)?(\s+)/", '$1.$2.$3', $valgrind_header, 1, $replace_count);
						if ($replace_count != 1) {
							error("Valgrind returned invalid version info (\"$valgrind_header\"), cannot proceed.");
						}
						$valgrind_header = trim($valgrind_header);
					}
					break;
				case 'n':
					if (!$pass_option_n) {
						$pass_options .= ' -n';
					}
					$pass_option_n = true;
					break;
				case 'e':
					$pass_options .= ' -e';
					break;
				case '--no-clean':
					$no_clean = true;
					break;
				case 'p':
					$php = $argv[++$i];
					putenv("TEST_PHP_EXECUTABLE=$php");
					$environment['TEST_PHP_EXECUTABLE'] = $php;
					break;
				case 'P':
					if(constant('PHP_BINARY')) {
						$php = PHP_BINARY;
					} else {
						break;
					}
					putenv("TEST_PHP_EXECUTABLE=$php");
					$environment['TEST_PHP_EXECUTABLE'] = $php;
					break;
				case 'q':
					putenv('NO_INTERACTION=1');
					break;
				//case 'r'
				case 's':
					$output_file = $argv[++$i];
					$just_save_results = true;
					break;
				case '--set-timeout':
					$environment['TEST_TIMEOUT'] = $argv[++$i];
					break;
				case '--show-all':
					foreach($cfgfiles as $file) {
						$cfg['show'][$file] = true;
					}
					break;
				case '--show-slow':
					$slow_min_ms = $argv[++$i];
					break;
				case '--temp-source':
					$temp_source = $argv[++$i];
					break;
				case '--temp-target':
					$temp_target = $argv[++$i];
					if ($temp_urlbase) {
						$temp_urlbase = $temp_target;
					}
					break;
				case '--temp-urlbase':
					$temp_urlbase = $argv[++$i];
					break;
				case 'v':
				case '--verbose':
					$DETAILED = true;
					break;
				case 'x':
					$environment['SKIP_SLOW_TESTS'] = 1;
					break;
				case '--offline':
					$environment['SKIP_ONLINE_TESTS'] = 1;
					break;
				//case 'w'
				case '-':
					// repeat check with full switch
					$switch = $argv[$i];
					if ($switch != '-') {
						$repeat = true;
					}
					break;
				case '--html':
					$html_file = fopen($argv[++$i], 'wt');
					$html_output = is_resource($html_file);
					break;
				case '--version':
					echo '$Id: f4720f5b76a811a5f7ee1b19ce15e4c68ecf7307 $' . "\n";
					exit(1);

				default:
					echo "Illegal switch '$switch' specified!\n";
				case 'h':
				case '-help':
				case '--help':
					echo <<<HELP
Synopsis:
    php run-tests.php [options] [files] [directories]

Options:
    -l <file>   Read the testfiles to be executed from <file>. After the test
                has finished all failed tests are written to the same <file>.
                If the list is empty and no further test is specified then
                all tests are executed (same as: -r <file> -w <file>).

    -r <file>   Read the testfiles to be executed from <file>.

    -w <file>   Write a list of all failed tests to <file>.

    -a <file>   Same as -w but append rather then truncating <file>.

    -W <file>   Write a list of all tests and their result status to <file>.

    -c <file>   Look for php.ini in directory <file> or use <file> as ini.

    -n          Pass -n option to the php binary (Do not use a php.ini).

    -d foo=bar  Pass -d option to the php binary (Define INI entry foo
                with value 'bar').

    -g          Comma separated list of groups to show during test run
                (possible values: PASS, FAIL, XFAIL, SKIP, BORK, WARN, LEAK, REDIRECT).

    -m          Test for memory leaks with Valgrind.

    -p <php>    Specify PHP executable to run.

    -P          Use PHP_BINARY as PHP executable to run.

    -q          Quiet, no user interaction (same as environment NO_INTERACTION).

    -s <file>   Write output to <file>.

    -x          Sets 'SKIP_SLOW_TESTS' environmental variable.

    --offline   Sets 'SKIP_ONLINE_TESTS' environmental variable.

    --verbose
    -v          Verbose mode.

    --help
    -h          This Help.

    --html <file> Generate HTML output.

    --temp-source <sdir>  --temp-target <tdir> [--temp-urlbase <url>]
                Write temporary files to <tdir> by replacing <sdir> from the
                filenames to generate with <tdir>. If --html is being used and
                <url> given then the generated links are relative and prefixed
                with the given url. In general you want to make <sdir> the path
                to your source files and <tdir> some pach in your web page
                hierarchy with <url> pointing to <tdir>.

    --keep-[all|php|skip|clean]
                Do not delete 'all' files, 'php' test file, 'skip' or 'clean'
                file.

    --set-timeout [n]
                Set timeout for individual tests, where [n] is the number of
                seconds. The default value is 60 seconds, or 300 seconds when
                testing for memory leaks.

    --show-[all|php|skip|clean|exp|diff|out]
                Show 'all' files, 'php' test file, 'skip' or 'clean' file. You
                can also use this to show the output 'out', the expected result
                'exp' or the difference between them 'diff'. The result types
                get written independent of the log format, however 'diff' only
                exists when a test fails.

    --show-slow [n]
                Show all tests that took longer than [n] milliseconds to run.

    --no-clean  Do not execute clean section if any.

HELP;
					exit(1);
			}
		}

		if (!$is_switch) {
			$testfile = realpath($argv[$i]);

			if (!$testfile && strpos($argv[$i], '*') !== false && function_exists('glob')) {

				if (preg_match("/\.phpt$/", $argv[$i])) {
					$pattern_match = glob($argv[$i]);
				} else if (preg_match("/\*$/", $argv[$i])) {
					$pattern_match = glob($argv[$i] . '.phpt');
				} else {
					die("bogus test name " . $argv[$i] . "\n");
				}

				if (is_array($pattern_match)) {
					$test_files = array_merge($test_files, $pattern_match);
				}

			} else if (is_dir($testfile)) {
				find_files($testfile);
			} else if (preg_match("/\.phpt$/", $testfile)) {
				$test_files[] = $testfile;
			} else {
				die("bogus test name " . $argv[$i] . "\n");
			}
		}
	}

	if (strlen($conf_passed)) {
		if (substr(PHP_OS, 0, 3) == "WIN") {
			$pass_options .= " -c " . escapeshellarg($conf_passed);
		} else {
			$pass_options .= " -c '$conf_passed'";
		}
	}

	$test_files = array_unique($test_files);
	$test_files = array_merge($test_files, $redir_tests);

	// Run selected tests.
	$test_cnt = count($test_files);

	if ($test_cnt) {
		putenv('NO_INTERACTION=1');
		verify_config();
		write_information();
		usort($test_files, "test_sort");
		$start_time = time();

		if (!$html_output) {
			echo "Running selected tests.\n";
		} else {
			show_start($start_time);
		}

		$test_idx = 0;
		run_all_tests($test_files, $environment);
		$end_time = time();

		if ($html_output) {
			show_end($end_time);
		}

		if ($failed_tests_file) {
			fclose($failed_tests_file);
		}

		if ($result_tests_file) {
			fclose($result_tests_file);
		}

		compute_summary();
		if ($html_output) {
			fwrite($html_file, "<hr/>\n" . get_summary(false, true));
		}
		echo "=====================================================================";
		echo get_summary(false, false);

		if ($html_output) {
			fclose($html_file);
		}

		if ($output_file != '' && $just_save_results) {
			save_or_mail_results();
		}

		junit_save_xml();

		if (getenv('REPORT_EXIT_STATUS') !== '0' &&
			getenv('REPORT_EXIT_STATUS') !== 'no' &&
			($sum_results['FAILED'] || $sum_results['BORKED'])) {
			exit(1);
		}

		exit(0);
	}
}

verify_config();
write_information();

// Compile a list of all test files (*.phpt).
$test_files = array();
$exts_tested = count($exts_to_test);
$exts_skipped = 0;
$ignored_by_ext = 0;
sort($exts_to_test);
$test_dirs = array();
$optionals = array('tests', 'ext', 'Zend', 'sapi');

foreach($optionals as $dir) {
	if (@filetype($dir) == 'dir') {
		$test_dirs[] = $dir;
	}
}

// Convert extension names to lowercase
foreach ($exts_to_test as $key => $val) {
	$exts_to_test[$key] = strtolower($val);
}

foreach ($test_dirs as $dir) {
	find_files("{$cwd}/{$dir}", ($dir == 'ext'));
}

foreach ($user_tests as $dir) {
	find_files($dir, ($dir == 'ext'));
}

function find_files($dir, $is_ext_dir = false, $ignore = false)
{
	global $test_files, $exts_to_test, $ignored_by_ext, $exts_skipped;

	$o = opendir($dir) or error("cannot open directory: $dir");

	while (($name = readdir($o)) !== false) {

		if (is_dir("{$dir}/{$name}") && !in_array($name, array('.', '..', '.svn'))) {
			$skip_ext = ($is_ext_dir && !in_array(strtolower($name), $exts_to_test));
			if ($skip_ext) {
				$exts_skipped++;
			}
			find_files("{$dir}/{$name}", false, $ignore || $skip_ext);
		}

		// Cleanup any left-over tmp files from last run.
		if (substr($name, -4) == '.tmp') {
			@unlink("$dir/$name");
			continue;
		}

		// Otherwise we're only interested in *.phpt files.
		if (substr($name, -5) == '.phpt') {
			if ($ignore) {
				$ignored_by_ext++;
			} else {
				$testfile = realpath("{$dir}/{$name}");
				$test_files[] = $testfile;
			}
		}
	}

	closedir($o);
}

function test_name($name)
{
	if (is_array($name)) {
		return $name[0] . ':' . $name[1];
	} else {
		return $name;
	}
}

function test_sort($a, $b)
{
	global $cwd;

	$a = test_name($a);
	$b = test_name($b);

	$ta = strpos($a, "{$cwd}/tests") === 0 ? 1 + (strpos($a, "{$cwd}/tests/run-test") === 0 ? 1 : 0) : 0;
	$tb = strpos($b, "{$cwd}/tests") === 0 ? 1 + (strpos($b, "{$cwd}/tests/run-test") === 0 ? 1 : 0) : 0;

	if ($ta == $tb) {
		return strcmp($a, $b);
	} else {
		return $tb - $ta;
	}
}

$test_files = array_unique($test_files);
usort($test_files, "test_sort");

$start_time = time();
show_start($start_time);

$test_cnt = count($test_files);
$test_idx = 0;
run_all_tests($test_files, $environment);
$end_time = time();

if ($failed_tests_file) {
	fclose($failed_tests_file);
}

if ($result_tests_file) {
	fclose($result_tests_file);
}

// Summarize results

if (0 == count($test_results)) {
	echo "No tests were run.\n";
	return;
}

compute_summary();

show_end($end_time);
show_summary();

if ($html_output) {
	fclose($html_file);
}

save_or_mail_results();

junit_save_xml();

if (getenv('REPORT_EXIT_STATUS') !== '0' &&
	getenv('REPORT_EXIT_STATUS') !== 'no' &&
	($sum_results['FAILED'] || $sum_results['BORKED'])) {
	exit(1);
}
exit(0);

//
// Send Email to QA Team
//

function mail_qa_team($data, $status = false)
{
	$url_bits = parse_url(QA_SUBMISSION_PAGE);

	if (($proxy = getenv('http_proxy'))) {
		$proxy = parse_url($proxy);
		$path = $url_bits['host'].$url_bits['path'];
		$host = $proxy['host'];
		if (empty($proxy['port'])) {
			$proxy['port'] = 80;
		}
		$port = $proxy['port'];
	} else {
		$path = $url_bits['path'];
		$host = $url_bits['host'];
		$port = empty($url_bits['port']) ? 80 : $port = $url_bits['port'];
	}

	$data = "php_test_data=" . urlencode(base64_encode(str_replace("\00", '[0x0]', $data)));
	$data_length = strlen($data);

	$fs = fsockopen($host, $port, $errno, $errstr, 10);

	if (!$fs) {
		return false;
	}

	$php_version = urlencode(TESTED_PHP_VERSION);

	echo "\nPosting to ". QA_SUBMISSION_PAGE . "\n";
	fwrite($fs, "POST " . $path . "?status=$status&version=$php_version HTTP/1.1\r\n");
	fwrite($fs, "Host: " . $host . "\r\n");
	fwrite($fs, "User-Agent: QA Browser 0.1\r\n");
	fwrite($fs, "Content-Type: application/x-www-form-urlencoded\r\n");
	fwrite($fs, "Content-Length: " . $data_length . "\r\n\r\n");
	fwrite($fs, $data);
	fwrite($fs, "\r\n\r\n");
	fclose($fs);

	return 1;
}


//
//  Write the given text to a temporary file, and return the filename.
//

function save_text($filename, $text, $filename_copy = null)
{
	global $DETAILED;

	if ($filename_copy && $filename_copy != $filename) {
		if (file_put_contents($filename_copy, $text, FILE_BINARY) === false) {
			error("Cannot open file '" . $filename_copy . "' (save_text)");
		}
	}

	if (file_put_contents($filename, $text, FILE_BINARY) === false) {
		error("Cannot open file '" . $filename . "' (save_text)");
	}

	if (1 < $DETAILED) echo "
FILE $filename {{{
$text
}}}
";
}

//
//  Write an error in a format recognizable to Emacs or MSVC.
//

function error_report($testname, $logname, $tested)
{
	$testname = realpath($testname);
	$logname  = realpath($logname);

	switch (strtoupper(getenv('TEST_PHP_ERROR_STYLE'))) {
		case 'MSVC':
			echo $testname . "(1) : $tested\n";
			echo $logname . "(1) :  $tested\n";
			break;
		case 'EMACS':
			echo $testname . ":1: $tested\n";
			echo $logname . ":1:  $tested\n";
			break;
	}
}

function system_with_timeout($commandline, $env = null, $stdin = null, $captureStdIn = true, $captureStdOut = true, $captureStdErr = true)
{
	global $leak_check, $cwd;

	$data = '';

	$bin_env = array();
	foreach((array)$env as $key => $value) {
		$bin_env[$key] = $value;
	}

	$descriptorspec = array();
	if ($captureStdIn) {
		$descriptorspec[0] = array('pipe', 'r');
	}
	if ($captureStdOut) {
		$descriptorspec[1] = array('pipe', 'w');
	}
	if ($captureStdErr) {
		$descriptorspec[2] = array('pipe', 'w');
	}
	$proc = proc_open($commandline, $descriptorspec, $pipes, $cwd, $bin_env, array('suppress_errors' => true, 'binary_pipes' => true));

	if (!$proc) {
		return false;
	}

	if ($captureStdIn) {
		if (!is_null($stdin)) {
			fwrite($pipes[0], $stdin);
		}
		fclose($pipes[0]);
		unset($pipes[0]);
	}

	$timeout = $leak_check ? 300 : (isset($env['TEST_TIMEOUT']) ? $env['TEST_TIMEOUT'] : 60);

	while (true) {
		/* hide errors from interrupted syscalls */
		$r = $pipes;
		$w = null;
		$e = null;

		$n = @stream_select($r, $w, $e, $timeout);

		if ($n === false) {
			break;
		} else if ($n === 0) {
			/* timed out */
			$data .= "\n ** ERROR: process timed out **\n";
			proc_terminate($proc, 9);
			return $data;
		} else if ($n > 0) {
			if ($captureStdOut) {
				$line = fread($pipes[1], 8192);
			} elseif ($captureStdErr) {
				$line = fread($pipes[2], 8192);
			} else {
				$line = '';
			}
			if (strlen($line) == 0) {
				/* EOF */
				break;
			}
			$data .= $line;
		}
	}

	$stat = proc_get_status($proc);

	if ($stat['signaled']) {
		$data .= "\nTermsig=" . $stat['stopsig'] . "\n";
	}
	if ($stat["exitcode"] > 128 && $stat["exitcode"] < 160) {
		$data .= "\nTermsig=" . ($stat["exitcode"] - 128) . "\n";
	}

	proc_close($proc);
	return $data;
}

function run_all_tests($test_files, $env, $redir_tested = null)
{
	global $test_results, $failed_tests_file, $result_tests_file, $php, $test_idx;

	foreach($test_files as $name) {

		if (is_array($name)) {
			$index = "# $name[1]: $name[0]";

			if ($redir_tested) {
				$name = $name[0];
			}
		} else if ($redir_tested) {
			$index = "# $redir_tested: $name";
		} else {
			$index = $name;
		}
		$test_idx++;
		$result = run_test($php, $name, $env);

		if (!is_array($name) && $result != 'REDIR') {
			$test_results[$index] = $result;
			if ($failed_tests_file && ($result == 'XFAILED' || $result == 'FAILED' || $result == 'WARNED' || $result == 'LEAKED')) {
				fwrite($failed_tests_file, "$index\n");
			}
			if ($result_tests_file) {
				fwrite($result_tests_file, "$result\t$index\n");
			}
		}
	}
}

//
//  Show file or result block
//
function show_file_block($file, $block, $section = null)
{
	global $cfg;

	if ($cfg['show'][$file]) {

		if (is_null($section)) {
			$section = strtoupper($file);
		}

		echo "\n========" . $section . "========\n";
		echo rtrim($block);
		echo "\n========DONE========\n";
	}
}

//
//  Run an individual test case.
//
function run_test($php, $file, $env)
{
	global $log_format, $ini_overwrites, $cwd, $PHP_FAILED_TESTS;
	global $pass_options, $DETAILED, $IN_REDIRECT, $test_cnt, $test_idx;
	global $leak_check, $temp_source, $temp_target, $cfg, $environment;
	global $no_clean;
	global $valgrind_version;
	global $SHOW_ONLY_GROUPS;
	global $no_file_cache;
	global $slow_min_ms;
	$temp_filenames = null;
	$org_file = $file;

	if (isset($env['TEST_PHP_CGI_EXECUTABLE'])) {
		$php_cgi = $env['TEST_PHP_CGI_EXECUTABLE'];
	}

	if (isset($env['TEST_PHPDBG_EXECUTABLE'])) {
		$phpdbg = $env['TEST_PHPDBG_EXECUTABLE'];
	}

	if (is_array($file)) {
		$file = $file[0];
	}

	if ($DETAILED) echo "
=================
TEST $file
";

	// Load the sections of the test file.
	$section_text = array('TEST' => '');

	$fp = fopen($file, "rb") or error("Cannot open test file: $file");

	$borked = false;
	$bork_info = '';

	if (!feof($fp)) {
		$line = fgets($fp);

		if ($line === false) {
			$bork_info = "cannot read test";
			$borked = true;
		}
	} else {
		$bork_info = "empty test [$file]";
		$borked = true;
	}
	if (!$borked && strncmp('--TEST--', $line, 8)) {
		$bork_info = "tests must start with --TEST-- [$file]";
		$borked = true;
	}

	$section = 'TEST';
	$secfile = false;
	$secdone = false;

	while (!feof($fp)) {
		$line = fgets($fp);

		if ($line === false) {
			break;
		}

		// Match the beginning of a section.
		if (preg_match('/^--([_A-Z]+)--/', $line, $r)) {
			$section = $r[1];
			settype($section, 'string');

			if (isset($section_text[$section])) {
				$bork_info = "duplicated $section section";
				$borked    = true;
			}

			$section_text[$section] = '';
			$secfile = $section == 'FILE' || $section == 'FILEEOF' || $section == 'FILE_EXTERNAL';
			$secdone = false;
			continue;
		}

		// Add to the section text.
		if (!$secdone) {
			$section_text[$section] .= $line;
		}

		// End of actual test?
		if ($secfile && preg_match('/^===DONE===\s*$/', $line)) {
			$secdone = true;
		}
	}

	// the redirect section allows a set of tests to be reused outside of
	// a given test dir
	if (!$borked) {
		if (@count($section_text['REDIRECTTEST']) == 1) {

			if ($IN_REDIRECT) {
				$borked = true;
				$bork_info = "Can't redirect a test from within a redirected test";
			} else {
				$borked = false;
			}

		} else {

			if (!isset($section_text['PHPDBG']) && @count($section_text['FILE']) + @count($section_text['FILEEOF']) + @count($section_text['FILE_EXTERNAL']) != 1) {
				$bork_info = "missing section --FILE--";
				$borked = true;
			}

			if (@count($section_text['FILEEOF']) == 1) {
				$section_text['FILE'] = preg_replace("/[\r\n]+$/", '', $section_text['FILEEOF']);
				unset($section_text['FILEEOF']);
			}

			foreach (array( 'FILE', 'EXPECT', 'EXPECTF', 'EXPECTREGEX' ) as $prefix) {
				$key = $prefix . '_EXTERNAL';

				if (@count($section_text[$key]) == 1) {
					// don't allow tests to retrieve files from anywhere but this subdirectory
					$section_text[$key] = dirname($file) . '/' . trim(str_replace('..', '', $section_text[$key]));

					if (file_exists($section_text[$key])) {
						$section_text[$prefix] = file_get_contents($section_text[$key], FILE_BINARY);
						unset($section_text[$key]);
					} else {
						$bork_info = "could not load --" . $key . "-- " . dirname($file) . '/' . trim($section_text[$key]);
						$borked = true;
					}
				}
			}

			if ((@count($section_text['EXPECT']) + @count($section_text['EXPECTF']) + @count($section_text['EXPECTREGEX'])) != 1) {
				$bork_info = "missing section --EXPECT--, --EXPECTF-- or --EXPECTREGEX--";
				$borked = true;
			}
		}
	}
	fclose($fp);

	$shortname = str_replace($cwd . '/', '', $file);
	$tested_file = $shortname;

	if ($borked) {
		show_result("BORK", $bork_info, $tested_file);
		$PHP_FAILED_TESTS['BORKED'][] = array (
								'name'      => $file,
								'test_name' => '',
								'output'    => '',
								'diff'      => '',
								'info'      => "$bork_info [$file]",
		);

		junit_mark_test_as('BORK', $shortname, $tested_file, 0, $bork_info);
		return 'BORKED';
	}

	if (isset($section_text['CAPTURE_STDIO'])) {
		$captureStdIn = stripos($section_text['CAPTURE_STDIO'], 'STDIN') !== false;
		$captureStdOut = stripos($section_text['CAPTURE_STDIO'], 'STDOUT') !== false;
		$captureStdErr = stripos($section_text['CAPTURE_STDIO'], 'STDERR') !== false;
	} else {
		$captureStdIn = true;
		$captureStdOut = true;
		$captureStdErr = true;
	}
	if ($captureStdOut && $captureStdErr) {
		$cmdRedirect = ' 2>&1';
	} else {
		$cmdRedirect = '';
	}

	$tested = trim($section_text['TEST']);

	/* For GET/POST/PUT tests, check if cgi sapi is available and if it is, use it. */
	if (!empty($section_text['GET']) || !empty($section_text['POST']) || !empty($section_text['GZIP_POST']) || !empty($section_text['DEFLATE_POST']) || !empty($section_text['POST_RAW']) || !empty($section_text['PUT']) || !empty($section_text['COOKIE']) || !empty($section_text['EXPECTHEADERS'])) {
		if (isset($php_cgi)) {
			$old_php = $php;
			$php = $php_cgi . ' -C ';
		} else if (!strncasecmp(PHP_OS, "win", 3) && file_exists(dirname($php) . "/php-cgi.exe")) {
			$old_php = $php;
			$php = realpath(dirname($php) . "/php-cgi.exe") . ' -C ';
		} else {
			if (file_exists(dirname($php) . "/../../sapi/cgi/php-cgi")) {
				$old_php = $php;
				$php = realpath(dirname($php) . "/../../sapi/cgi/php-cgi") . ' -C ';
			} else if (file_exists("./sapi/cgi/php-cgi")) {
				$old_php = $php;
				$php = realpath("./sapi/cgi/php-cgi") . ' -C ';
			} else if (file_exists(dirname($php) . "/php-cgi")) {
				$old_php = $php;
				$php = realpath(dirname($php) . "/php-cgi") . ' -C ';
			} else {
				show_result('SKIP', $tested, $tested_file, "reason: CGI not available");

				junit_init_suite(junit_get_suitename_for($shortname));
				junit_mark_test_as('SKIP', $shortname, $tested, 0, 'CGI not available');
				return 'SKIPPED';
			}
		}
		$uses_cgi = true;
	}

	/* For phpdbg tests, check if phpdbg sapi is available and if it is, use it. */
	if (array_key_exists('PHPDBG', $section_text)) {
		if (!isset($section_text['STDIN'])) {
			$section_text['STDIN'] = $section_text['PHPDBG']."\n";
		}

		if (isset($phpdbg)) {
			$old_php = $php;
			$php = $phpdbg . ' -qIb';
		} else {
			show_result('SKIP', $tested, $tested_file, "reason: phpdbg not available");

			junit_init_suite(junit_get_suitename_for($shortname));
			junit_mark_test_as('SKIP', $shortname, $tested, 0, 'phpdbg not available');
			return 'SKIPPED';
		}
	}

	if (!$SHOW_ONLY_GROUPS) {
		show_test($test_idx, $shortname);
	}

	if (is_array($IN_REDIRECT)) {
		$temp_dir = $test_dir = $IN_REDIRECT['dir'];
	} else {
		$temp_dir = $test_dir = realpath(dirname($file));
	}

	if ($temp_source && $temp_target) {
		$temp_dir = str_replace($temp_source, $temp_target, $temp_dir);
	}

	$main_file_name = basename($file,'phpt');

	$diff_filename     = $temp_dir . DIRECTORY_SEPARATOR . $main_file_name . 'diff';
	$log_filename      = $temp_dir . DIRECTORY_SEPARATOR . $main_file_name . 'log';
	$exp_filename      = $temp_dir . DIRECTORY_SEPARATOR . $main_file_name . 'exp';
	$output_filename   = $temp_dir . DIRECTORY_SEPARATOR . $main_file_name . 'out';
	$memcheck_filename = $temp_dir . DIRECTORY_SEPARATOR . $main_file_name . 'mem';
	$sh_filename       = $temp_dir . DIRECTORY_SEPARATOR . $main_file_name . 'sh';
	$temp_file         = $temp_dir . DIRECTORY_SEPARATOR . $main_file_name . 'php';
	$test_file         = $test_dir . DIRECTORY_SEPARATOR . $main_file_name . 'php';
	$temp_skipif       = $temp_dir . DIRECTORY_SEPARATOR . $main_file_name . 'skip.php';
	$test_skipif       = $test_dir . DIRECTORY_SEPARATOR . $main_file_name . 'skip.php';
	$temp_clean        = $temp_dir . DIRECTORY_SEPARATOR . $main_file_name . 'clean.php';
	$test_clean        = $test_dir . DIRECTORY_SEPARATOR . $main_file_name . 'clean.php';
	$tmp_post          = $temp_dir . DIRECTORY_SEPARATOR . uniqid('/phpt.');
	$tmp_relative_file = str_replace(__DIR__ . DIRECTORY_SEPARATOR, '', $test_file) . 't';

	if ($temp_source && $temp_target) {
		$temp_skipif  .= 's';
		$temp_file    .= 's';
		$temp_clean   .= 's';
		$copy_file     = $temp_dir . DIRECTORY_SEPARATOR . basename(is_array($file) ? $file[1] : $file) . '.phps';

		if (!is_dir(dirname($copy_file))) {
			mkdir(dirname($copy_file), 0777, true) or error("Cannot create output directory - " . dirname($copy_file));
		}

		if (isset($section_text['FILE'])) {
			save_text($copy_file, $section_text['FILE']);
		}

		$temp_filenames = array(
			'file' => $copy_file,
			'diff' => $diff_filename,
			'log'  => $log_filename,
			'exp'  => $exp_filename,
			'out'  => $output_filename,
			'mem'  => $memcheck_filename,
			'sh'   => $sh_filename,
			'php'  => $temp_file,
			'skip' => $temp_skipif,
			'clean'=> $temp_clean);
	}

	if (is_array($IN_REDIRECT)) {
		$tested = $IN_REDIRECT['prefix'] . ' ' . trim($section_text['TEST']);
		$tested_file = $tmp_relative_file;
	}

	// unlink old test results
	@unlink($diff_filename);
	@unlink($log_filename);
	@unlink($exp_filename);
	@unlink($output_filename);
	@unlink($memcheck_filename);
	@unlink($sh_filename);
	@unlink($temp_file);
	@unlink($test_file);
	@unlink($temp_skipif);
	@unlink($test_skipif);
	@unlink($tmp_post);
	@unlink($temp_clean);
	@unlink($test_clean);

	// Reset environment from any previous test.
	$env['REDIRECT_STATUS'] = '';
	$env['QUERY_STRING']    = '';
	$env['PATH_TRANSLATED'] = '';
	$env['SCRIPT_FILENAME'] = '';
	$env['REQUEST_METHOD']  = '';
	$env['CONTENT_TYPE']    = '';
	$env['CONTENT_LENGTH']  = '';
	$env['TZ']              = '';

	if (!empty($section_text['ENV'])) {

		foreach(explode("\n", trim($section_text['ENV'])) as $e) {
			$e = explode('=', trim($e), 2);

			if (!empty($e[0]) && isset($e[1])) {
				$env[$e[0]] = $e[1];
			}
		}
	}

	// Default ini settings
	$ini_settings = array();

	// Additional required extensions
	if (array_key_exists('EXTENSIONS', $section_text)) {
		$ext_params = array();
		settings2array($ini_overwrites, $ext_params);
		settings2params($ext_params);
		$ext_dir=`$php $pass_options $ext_params -d display_errors=0 -r "echo ini_get('extension_dir');"`;
		$extensions = preg_split("/[\n\r]+/", trim($section_text['EXTENSIONS']));
		$loaded = explode(",", `$php $pass_options $ext_params -d display_errors=0 -r "echo implode(',', get_loaded_extensions());"`);
		$ext_prefix = substr(PHP_OS, 0, 3) === "WIN" ? "php_" : "";
		foreach ($extensions as $req_ext) {
			if (!in_array($req_ext, $loaded)) {
				if ($req_ext == 'opcache') {
					$ini_settings['zend_extension'][] = $ext_dir . DIRECTORY_SEPARATOR . $ext_prefix . $req_ext . '.' . PHP_SHLIB_SUFFIX;
				} else {
					$ini_settings['extension'][] = $ext_dir . DIRECTORY_SEPARATOR . $ext_prefix . $req_ext . '.' . PHP_SHLIB_SUFFIX;
				}
			}
		}
	}

	// additional ini overwrites
	//$ini_overwrites[] = 'setting=value';
	settings2array($ini_overwrites, $ini_settings);

	// Any special ini settings
	// these may overwrite the test defaults...
	if (array_key_exists('INI', $section_text)) {
		if (strpos($section_text['INI'], '{PWD}') !== false) {
			$section_text['INI'] = str_replace('{PWD}', dirname($file), $section_text['INI']);
		}
		settings2array(preg_split( "/[\n\r]+/", $section_text['INI']), $ini_settings);
	}

	settings2params($ini_settings);

	// Check if test should be skipped.
	$info = '';
	$warn = false;

	if (array_key_exists('SKIPIF', $section_text)) {

		if (trim($section_text['SKIPIF'])) {
			show_file_block('skip', $section_text['SKIPIF']);
			save_text($test_skipif, $section_text['SKIPIF'], $temp_skipif);
			$extra = substr(PHP_OS, 0, 3) !== "WIN" ?
				"unset REQUEST_METHOD; unset QUERY_STRING; unset PATH_TRANSLATED; unset SCRIPT_FILENAME; unset REQUEST_METHOD;": "";

			if ($leak_check) {
				$env['USE_ZEND_ALLOC'] = '0';
				$env['ZEND_DONT_UNLOAD_MODULES'] = 1;
			} else {
				$env['USE_ZEND_ALLOC'] = '1';
				$env['ZEND_DONT_UNLOAD_MODULES'] = 0;
			}

			junit_start_timer($shortname);

			$output = system_with_timeout("$extra $php $pass_options -q $ini_settings $no_file_cache -d display_errors=0 \"$test_skipif\"", $env);

			junit_finish_timer($shortname);

			if (!$cfg['keep']['skip']) {
				@unlink($test_skipif);
			}

			if (!strncasecmp('skip', ltrim($output), 4)) {

				if (preg_match('/^\s*skip\s*(.+)\s*/i', $output, $m)) {
					show_result('SKIP', $tested, $tested_file, "reason: $m[1]", $temp_filenames);
				} else {
					show_result('SKIP', $tested, $tested_file, '', $temp_filenames);
				}

				if (!$cfg['keep']['skip']) {
					@unlink($test_skipif);
				}

				$message = !empty($m[1]) ? $m[1] : '';
				junit_mark_test_as('SKIP', $shortname, $tested, null, $message);
				return 'SKIPPED';
			}

			if (!strncasecmp('info', ltrim($output), 4)) {
				if (preg_match('/^\s*info\s*(.+)\s*/i', $output, $m)) {
					$info = " (info: $m[1])";
				}
			}

			if (!strncasecmp('warn', ltrim($output), 4)) {
				if (preg_match('/^\s*warn\s*(.+)\s*/i', $output, $m)) {
					$warn = true; /* only if there is a reason */
					$info = " (warn: $m[1])";
				}
			}

			if (!strncasecmp('xfail', ltrim($output), 5)) {
				// Pretend we have an XFAIL section
				$section_text['XFAIL'] = trim(substr(ltrim($output), 5));
			}
		}
	}

	if (!extension_loaded("zlib")
	&& (	array_key_exists("GZIP_POST", $section_text)
		||	array_key_exists("DEFLATE_POST", $section_text))
	) {
		$message = "ext/zlib required";
		show_result('SKIP', $tested, $tested_file, "reason: $message", $temp_filenames);
		junit_mark_test_as('SKIP', $shortname, $tested, null, $message);
		return 'SKIPPED';
	}

	if (@count($section_text['REDIRECTTEST']) == 1) {
		$test_files = array();

		$IN_REDIRECT = eval($section_text['REDIRECTTEST']);
		$IN_REDIRECT['via'] = "via [$shortname]\n\t";
		$IN_REDIRECT['dir'] = realpath(dirname($file));
		$IN_REDIRECT['prefix'] = trim($section_text['TEST']);

		if (!empty($IN_REDIRECT['TESTS'])) {

			if (is_array($org_file)) {
				$test_files[] = $org_file[1];
			} else {
				$GLOBALS['test_files'] = $test_files;
				find_files($IN_REDIRECT['TESTS']);

				foreach($GLOBALS['test_files'] as $f) {
					$test_files[] = array($f, $file);
				}
			}
			$test_cnt += @count($test_files) - 1;
			$test_idx--;

			show_redirect_start($IN_REDIRECT['TESTS'], $tested, $tested_file);

			// set up environment
			$redirenv = array_merge($environment, $IN_REDIRECT['ENV']);
			$redirenv['REDIR_TEST_DIR'] = realpath($IN_REDIRECT['TESTS']) . DIRECTORY_SEPARATOR;

			usort($test_files, "test_sort");
			run_all_tests($test_files, $redirenv, $tested);

			show_redirect_ends($IN_REDIRECT['TESTS'], $tested, $tested_file);

			// a redirected test never fails
			$IN_REDIRECT = false;

			junit_mark_test_as('PASS', $shortname, $tested);
			return 'REDIR';

		} else {

			$bork_info = "Redirect info must contain exactly one TEST string to be used as redirect directory.";
			show_result("BORK", $bork_info, '', $temp_filenames);
			$PHP_FAILED_TESTS['BORKED'][] = array (
									'name' => $file,
									'test_name' => '',
									'output' => '',
									'diff'   => '',
									'info'   => "$bork_info [$file]",
			);
		}
	}

	if (is_array($org_file) || @count($section_text['REDIRECTTEST']) == 1) {

		if (is_array($org_file)) {
			$file = $org_file[0];
		}

		$bork_info = "Redirected test did not contain redirection info";
		show_result("BORK", $bork_info, '', $temp_filenames);
		$PHP_FAILED_TESTS['BORKED'][] = array (
								'name' => $file,
								'test_name' => '',
								'output' => '',
								'diff'   => '',
								'info'   => "$bork_info [$file]",
		);

		junit_mark_test_as('BORK', $shortname, $tested, null, $bork_info);

		return 'BORKED';
	}

	// We've satisfied the preconditions - run the test!
	if (isset($section_text['FILE'])) {
		show_file_block('php', $section_text['FILE'], 'TEST');
		save_text($test_file, $section_text['FILE'], $temp_file);
	} else {
		$test_file = $temp_file = "";
	}

	if (array_key_exists('GET', $section_text)) {
		$query_string = trim($section_text['GET']);
	} else {
		$query_string = '';
	}

	$env['REDIRECT_STATUS'] = '1';
	if (empty($env['QUERY_STRING'])) {
		$env['QUERY_STRING']    = $query_string;
	}
	if (empty($env['PATH_TRANSLATED'])) {
		$env['PATH_TRANSLATED'] = $test_file;
	}
	if (empty($env['SCRIPT_FILENAME'])) {
		$env['SCRIPT_FILENAME'] = $test_file;
	}

	if (array_key_exists('COOKIE', $section_text)) {
		$env['HTTP_COOKIE'] = trim($section_text['COOKIE']);
	} else {
		$env['HTTP_COOKIE'] = '';
	}

	$args = isset($section_text['ARGS']) ? ' -- ' . $section_text['ARGS'] : '';

	if (array_key_exists('POST_RAW', $section_text) && !empty($section_text['POST_RAW'])) {

		$post = trim($section_text['POST_RAW']);
		$raw_lines = explode("\n", $post);

		$request = '';
		$started = false;

		foreach ($raw_lines as $line) {

			if (empty($env['CONTENT_TYPE']) && preg_match('/^Content-Type:(.*)/i', $line, $res)) {
				$env['CONTENT_TYPE'] = trim(str_replace("\r", '', $res[1]));
				continue;
			}

			if ($started) {
				$request .= "\n";
			}

			$started = true;
			$request .= $line;
		}

		$env['CONTENT_LENGTH'] = strlen($request);
		$env['REQUEST_METHOD'] = 'POST';

		if (empty($request)) {
			junit_mark_test_as('BORK', $shortname, $tested, null, 'empty $request');
			return 'BORKED';
		}

		save_text($tmp_post, $request);
		$cmd = "$php $pass_options $ini_settings -f \"$test_file\"$cmdRedirect < \"$tmp_post\"";

	} elseif (array_key_exists('PUT', $section_text) && !empty($section_text['PUT'])) {

		$post = trim($section_text['PUT']);
		$raw_lines = explode("\n", $post);

		$request = '';
		$started = false;

		foreach ($raw_lines as $line) {

			if (empty($env['CONTENT_TYPE']) && preg_match('/^Content-Type:(.*)/i', $line, $res)) {
				$env['CONTENT_TYPE'] = trim(str_replace("\r", '', $res[1]));
				continue;
			}

			if ($started) {
				$request .= "\n";
			}

			$started = true;
			$request .= $line;
		}

		$env['CONTENT_LENGTH'] = strlen($request);
		$env['REQUEST_METHOD'] = 'PUT';

		if (empty($request)) {
			junit_mark_test_as('BORK', $shortname, $tested, null, 'empty $request');
			return 'BORKED';
		}

		save_text($tmp_post, $request);
		$cmd = "$php $pass_options $ini_settings -f \"$test_file\"$cmdRedirect < \"$tmp_post\"";

	} else if (array_key_exists('POST', $section_text) && !empty($section_text['POST'])) {

		$post = trim($section_text['POST']);
		$content_length = strlen($post);
		save_text($tmp_post, $post);

		$env['REQUEST_METHOD'] = 'POST';
		if (empty($env['CONTENT_TYPE'])) {
			$env['CONTENT_TYPE']   = 'application/x-www-form-urlencoded';
		}

		if (empty($env['CONTENT_LENGTH'])) {
			$env['CONTENT_LENGTH'] = $content_length;
		}

		$cmd = "$php $pass_options $ini_settings -f \"$test_file\"$cmdRedirect < \"$tmp_post\"";

	} else if (array_key_exists('GZIP_POST', $section_text) && !empty($section_text['GZIP_POST'])) {

		$post = trim($section_text['GZIP_POST']);
		$post = gzencode($post, 9, FORCE_GZIP);
		$env['HTTP_CONTENT_ENCODING'] = 'gzip';

		save_text($tmp_post, $post);
		$content_length = strlen($post);

		$env['REQUEST_METHOD'] = 'POST';
		$env['CONTENT_TYPE']   = 'application/x-www-form-urlencoded';
		$env['CONTENT_LENGTH'] = $content_length;

		$cmd = "$php $pass_options $ini_settings -f \"$test_file\"$cmdRedirect < \"$tmp_post\"";

	} else if (array_key_exists('DEFLATE_POST', $section_text) && !empty($section_text['DEFLATE_POST'])) {
		$post = trim($section_text['DEFLATE_POST']);
		$post = gzcompress($post, 9);
		$env['HTTP_CONTENT_ENCODING'] = 'deflate';
		save_text($tmp_post, $post);
		$content_length = strlen($post);

		$env['REQUEST_METHOD'] = 'POST';
		$env['CONTENT_TYPE']   = 'application/x-www-form-urlencoded';
		$env['CONTENT_LENGTH'] = $content_length;

		$cmd = "$php $pass_options $ini_settings -f \"$test_file\"$cmdRedirect < \"$tmp_post\"";

	} else {

		$env['REQUEST_METHOD'] = 'GET';
		$env['CONTENT_TYPE']   = '';
		$env['CONTENT_LENGTH'] = '';

		$cmd = "$php $pass_options $ini_settings -f \"$test_file\" $args$cmdRedirect";
	}

	if ($leak_check) {
		$env['USE_ZEND_ALLOC'] = '0';
		$env['ZEND_DONT_UNLOAD_MODULES'] = 1;

		$valgrind_cmd = "valgrind -q --tool=memcheck --trace-children=yes";
		if (strpos($test_file, "pcre") !== false) {
			$valgrind_cmd .= " --smc-check=all";
		}

		/* --vex-iropt-register-updates=allregs-at-mem-access is necessary for phpdbg watchpoint tests */
		if (version_compare($valgrind_version, '3.8.0', '>=')) {
			/* valgrind 3.3.0+ doesn't have --log-file-exactly option */
			$cmd = "$valgrind_cmd --vex-iropt-register-updates=allregs-at-mem-access --log-file=$memcheck_filename $cmd";
		} elseif (version_compare($valgrind_version, '3.3.0', '>=')) {
			$cmd = "$valgrind_cmd --vex-iropt-precise-memory-exns=yes --log-file=$memcheck_filename $cmd";
		} else {
			$cmd = "$valgrind_cmd --vex-iropt-precise-memory-exns=yes --log-file-exactly=$memcheck_filename $cmd";
		}

	} else {
		$env['USE_ZEND_ALLOC'] = '1';
		$env['ZEND_DONT_UNLOAD_MODULES'] = 0;
	}

	if ($DETAILED) echo "
CONTENT_LENGTH  = " . $env['CONTENT_LENGTH'] . "
CONTENT_TYPE    = " . $env['CONTENT_TYPE'] . "
PATH_TRANSLATED = " . $env['PATH_TRANSLATED'] . "
QUERY_STRING    = " . $env['QUERY_STRING'] . "
REDIRECT_STATUS = " . $env['REDIRECT_STATUS'] . "
REQUEST_METHOD  = " . $env['REQUEST_METHOD'] . "
SCRIPT_FILENAME = " . $env['SCRIPT_FILENAME'] . "
HTTP_COOKIE     = " . $env['HTTP_COOKIE'] . "
COMMAND $cmd
";

	junit_start_timer($shortname);
	$startTime = microtime(true);

	$out = system_with_timeout($cmd, $env, isset($section_text['STDIN']) ? $section_text['STDIN'] : null, $captureStdIn, $captureStdOut, $captureStdErr);

	junit_finish_timer($shortname);
	$time = microtime(true) - $startTime;
	if ($time * 1000 >= $slow_min_ms) {
		$PHP_FAILED_TESTS['SLOW'][] = array(
			'name'      => $file,
			'test_name' => (is_array($IN_REDIRECT) ? $IN_REDIRECT['via'] : '') . $tested . " [$tested_file]",
			'output'    => '',
			'diff'      => '',
			'info'      => $time,
		);
	}

	if (array_key_exists('CLEAN', $section_text) && (!$no_clean || $cfg['keep']['clean'])) {

		if (trim($section_text['CLEAN'])) {
			show_file_block('clean', $section_text['CLEAN']);
			save_text($test_clean, trim($section_text['CLEAN']), $temp_clean);

			if (!$no_clean) {
				$clean_params = array();
				settings2array($ini_overwrites, $clean_params);
				settings2params($clean_params);
				$extra = substr(PHP_OS, 0, 3) !== "WIN" ?
					"unset REQUEST_METHOD; unset QUERY_STRING; unset PATH_TRANSLATED; unset SCRIPT_FILENAME; unset REQUEST_METHOD;": "";
				system_with_timeout("$extra $php $pass_options -q $clean_params $no_file_cache \"$test_clean\"", $env);
			}

			if (!$cfg['keep']['clean']) {
				@unlink($test_clean);
			}
		}
	}

	@unlink($tmp_post);

	$leaked = false;
	$passed = false;

	if ($leak_check) { // leak check
		$leaked = filesize($memcheck_filename) > 0;

		if (!$leaked) {
			@unlink($memcheck_filename);
		}
	}

	// Does the output match what is expected?
	$output = preg_replace("/\r\n/", "\n", trim($out));

	/* when using CGI, strip the headers from the output */
	$headers = array();

	if (!empty($uses_cgi) && preg_match("/^(.*?)\r?\n\r?\n(.*)/s", $out, $match)) {
		$output = trim($match[2]);
		$rh = preg_split("/[\n\r]+/", $match[1]);

		foreach ($rh as $line) {
			if (strpos($line, ':') !== false) {
				$line = explode(':', $line, 2);
				$headers[trim($line[0])] = trim($line[1]);
			}
		}
	}

	$failed_headers = false;

	if (isset($section_text['EXPECTHEADERS'])) {
		$want = array();
		$wanted_headers = array();
		$lines = preg_split("/[\n\r]+/", $section_text['EXPECTHEADERS']);

		foreach($lines as $line) {
			if (strpos($line, ':') !== false) {
				$line = explode(':', $line, 2);
				$want[trim($line[0])] = trim($line[1]);
				$wanted_headers[] = trim($line[0]) . ': ' . trim($line[1]);
			}
		}

		$output_headers = array();

		foreach($want as $k => $v) {

			if (isset($headers[$k])) {
				$output_headers[] = $k . ': ' . $headers[$k];
			}

			if (!isset($headers[$k]) || $headers[$k] != $v) {
				$failed_headers = true;
			}
		}

		ksort($wanted_headers);
		$wanted_headers = implode("\n", $wanted_headers);
		ksort($output_headers);
		$output_headers = implode("\n", $output_headers);
	}

	show_file_block('out', $output);

	if (isset($section_text['EXPECTF']) || isset($section_text['EXPECTREGEX'])) {

		if (isset($section_text['EXPECTF'])) {
			$wanted = trim($section_text['EXPECTF']);
		} else {
			$wanted = trim($section_text['EXPECTREGEX']);
		}

		show_file_block('exp', $wanted);
		$wanted_re = preg_replace('/\r\n/', "\n", $wanted);

		if (isset($section_text['EXPECTF'])) {

			// do preg_quote, but miss out any %r delimited sections
			$temp = "";
			$r = "%r";
			$startOffset = 0;
			$length = strlen($wanted_re);
			while($startOffset < $length) {
				$start = strpos($wanted_re, $r, $startOffset);
				if ($start !== false) {
					// we have found a start tag
					$end = strpos($wanted_re, $r, $start+2);
					if ($end === false) {
						// unbalanced tag, ignore it.
						$end = $start = $length;
					}
				} else {
					// no more %r sections
					$start = $end = $length;
				}
				// quote a non re portion of the string
				$temp = $temp . preg_quote(substr($wanted_re, $startOffset, ($start - $startOffset)),  '/');
				// add the re unquoted.
				if ($end > $start) {
					$temp = $temp . '(' . substr($wanted_re, $start+2, ($end - $start-2)). ')';
				}
				$startOffset = $end + 2;
			}
			$wanted_re = $temp;

			$wanted_re = str_replace(
				array('%binary_string_optional%'),
				'string',
				$wanted_re
			);
			$wanted_re = str_replace(
				array('%unicode_string_optional%'),
				'string',
				$wanted_re
			);
			$wanted_re = str_replace(
				array('%unicode\|string%', '%string\|unicode%'),
				'string',
				$wanted_re
			);
			$wanted_re = str_replace(
				array('%u\|b%', '%b\|u%'),
				'',
				$wanted_re
			);
			// Stick to basics
			$wanted_re = str_replace('%e', '\\' . DIRECTORY_SEPARATOR, $wanted_re);
			$wanted_re = str_replace('%s', '[^\r\n]+', $wanted_re);
			$wanted_re = str_replace('%S', '[^\r\n]*', $wanted_re);
			$wanted_re = str_replace('%a', '.+', $wanted_re);
			$wanted_re = str_replace('%A', '.*', $wanted_re);
			$wanted_re = str_replace('%w', '\s*', $wanted_re);
			$wanted_re = str_replace('%i', '[+-]?\d+', $wanted_re);
			$wanted_re = str_replace('%d', '\d+', $wanted_re);
			$wanted_re = str_replace('%x', '[0-9a-fA-F]+', $wanted_re);
			$wanted_re = str_replace('%f', '[+-]?\.?\d+\.?\d*(?:[Ee][+-]?\d+)?', $wanted_re);
			$wanted_re = str_replace('%c', '.', $wanted_re);
			// %f allows two points "-.0.0" but that is the best *simple* expression
		}
/* DEBUG YOUR REGEX HERE
		var_dump($wanted_re);
		print(str_repeat('=', 80) . "\n");
		var_dump($output);
*/
		if (preg_match("/^$wanted_re\$/s", $output)) {
			$passed = true;
			if (!$cfg['keep']['php']) {
				@unlink($test_file);
			}

			if (!$leaked && !$failed_headers) {
				if (isset($section_text['XFAIL'] )) {
					$warn = true;
					$info = " (warn: XFAIL section but test passes)";
				}else {
					show_result("PASS", $tested, $tested_file, '', $temp_filenames);
					junit_mark_test_as('PASS', $shortname, $tested);
					return 'PASSED';
				}
			}
		}

	} else {

		$wanted = trim($section_text['EXPECT']);
		$wanted = preg_replace('/\r\n/',"\n", $wanted);
		show_file_block('exp', $wanted);

		// compare and leave on success
		if (!strcmp($output, $wanted)) {
			$passed = true;

			if (!$cfg['keep']['php']) {
				@unlink($test_file);
			}

			if (!$leaked && !$failed_headers) {
				if (isset($section_text['XFAIL'] )) {
					$warn = true;
					$info = " (warn: XFAIL section but test passes)";
				}else {
					show_result("PASS", $tested, $tested_file, '', $temp_filenames);
					junit_mark_test_as('PASS', $shortname, $tested);
					return 'PASSED';
				}
			}
		}

		$wanted_re = null;
	}

	// Test failed so we need to report details.
	if ($failed_headers) {
		$passed = false;
		$wanted = $wanted_headers . "\n--HEADERS--\n" . $wanted;
		$output = $output_headers . "\n--HEADERS--\n" . $output;

		if (isset($wanted_re)) {
			$wanted_re = preg_quote($wanted_headers . "\n--HEADERS--\n", '/') . $wanted_re;
		}
	}

	if ($leaked) {
		$restype[] = 'LEAK';
	}

	if ($warn) {
		$restype[] = 'WARN';
	}

	if (!$passed) {
		if (isset($section_text['XFAIL'])) {
			$restype[] = 'XFAIL';
			$info = '  XFAIL REASON: ' . rtrim($section_text['XFAIL']);
		} else {
			$restype[] = 'FAIL';
		}
	}

	if (!$passed) {

		// write .exp
		if (strpos($log_format, 'E') !== false && file_put_contents($exp_filename, $wanted, FILE_BINARY) === false) {
			error("Cannot create expected test output - $exp_filename");
		}

		// write .out
		if (strpos($log_format, 'O') !== false && file_put_contents($output_filename, $output, FILE_BINARY) === false) {
			error("Cannot create test output - $output_filename");
		}

		// write .diff
		$diff = generate_diff($wanted, $wanted_re, $output);
		if (is_array($IN_REDIRECT)) {
			$diff = "# original source file: $shortname\n" . $diff;
		}
		show_file_block('diff', $diff);
		if (strpos($log_format, 'D') !== false && file_put_contents($diff_filename, $diff, FILE_BINARY) === false) {
			error("Cannot create test diff - $diff_filename");
		}

		// write .sh
		if (strpos($log_format, 'S') !== false && file_put_contents($sh_filename, "#!/bin/sh

{$cmd}
", FILE_BINARY) === false) {
			error("Cannot create test shell script - $sh_filename");
		}
		chmod($sh_filename, 0755);

		// write .log
		if (strpos($log_format, 'L') !== false && file_put_contents($log_filename, "
---- EXPECTED OUTPUT
$wanted
---- ACTUAL OUTPUT
$output
---- FAILED
", FILE_BINARY) === false) {
			error("Cannot create test log - $log_filename");
			error_report($file, $log_filename, $tested);
		}
	}

	show_result(implode('&', $restype), $tested, $tested_file, $info, $temp_filenames);

	foreach ($restype as $type) {
		$PHP_FAILED_TESTS[$type.'ED'][] = array (
			'name'      => $file,
			'test_name' => (is_array($IN_REDIRECT) ? $IN_REDIRECT['via'] : '') . $tested . " [$tested_file]",
			'output'    => $output_filename,
			'diff'      => $diff_filename,
			'info'      => $info,
		);
	}

	$diff = empty($diff) ? '' : preg_replace('/\e/', '<esc>', $diff);

	junit_mark_test_as($restype, str_replace($cwd . '/', '', $tested_file), $tested, null, $info, $diff);

	return $restype[0] . 'ED';
}

function comp_line($l1, $l2, $is_reg)
{
	if ($is_reg) {
		return preg_match('/^'. $l1 . '$/s', $l2);
	} else {
		return !strcmp($l1, $l2);
	}
}

function count_array_diff($ar1, $ar2, $is_reg, $w, $idx1, $idx2, $cnt1, $cnt2, $steps)
{
	$equal = 0;

	while ($idx1 < $cnt1 && $idx2 < $cnt2 && comp_line($ar1[$idx1], $ar2[$idx2], $is_reg)) {
		$idx1++;
		$idx2++;
		$equal++;
		$steps--;
	}
	if (--$steps > 0) {
		$eq1 = 0;
		$st = $steps / 2;

		for ($ofs1 = $idx1 + 1; $ofs1 < $cnt1 && $st-- > 0; $ofs1++) {
			$eq = @count_array_diff($ar1, $ar2, $is_reg, $w, $ofs1, $idx2, $cnt1, $cnt2, $st);

			if ($eq > $eq1) {
				$eq1 = $eq;
			}
		}

		$eq2 = 0;
		$st = $steps;

		for ($ofs2 = $idx2 + 1; $ofs2 < $cnt2 && $st-- > 0; $ofs2++) {
			$eq = @count_array_diff($ar1, $ar2, $is_reg, $w, $idx1, $ofs2, $cnt1, $cnt2, $st);
			if ($eq > $eq2) {
				$eq2 = $eq;
			}
		}

		if ($eq1 > $eq2) {
			$equal += $eq1;
		} else if ($eq2 > 0) {
			$equal += $eq2;
		}
	}

	return $equal;
}

function generate_array_diff($ar1, $ar2, $is_reg, $w)
{
	$idx1 = 0; $cnt1 = @count($ar1);
	$idx2 = 0; $cnt2 = @count($ar2);
	$diff = array();
	$old1 = array();
	$old2 = array();

	while ($idx1 < $cnt1 && $idx2 < $cnt2) {

		if (comp_line($ar1[$idx1], $ar2[$idx2], $is_reg)) {
			$idx1++;
			$idx2++;
			continue;
		} else {

			$c1 = @count_array_diff($ar1, $ar2, $is_reg, $w, $idx1+1, $idx2, $cnt1, $cnt2, 10);
			$c2 = @count_array_diff($ar1, $ar2, $is_reg, $w, $idx1, $idx2+1, $cnt1,  $cnt2, 10);

			if ($c1 > $c2) {
				$old1[$idx1] = sprintf("%03d- ", $idx1+1) . $w[$idx1++];
			} else if ($c2 > 0) {
				$old2[$idx2] = sprintf("%03d+ ", $idx2+1) . $ar2[$idx2++];
			} else {
				$old1[$idx1] = sprintf("%03d- ", $idx1+1) . $w[$idx1++];
				$old2[$idx2] = sprintf("%03d+ ", $idx2+1) . $ar2[$idx2++];
			}
		}
	}

	reset($old1); $k1 = key($old1); $l1 = -2;
	reset($old2); $k2 = key($old2); $l2 = -2;

	while ($k1 !== null || $k2 !== null) {

		if ($k1 == $l1 + 1 || $k2 === null) {
			$l1 = $k1;
			$diff[] = current($old1);
			$k1 = next($old1) ? key($old1) : null;
		} else if ($k2 == $l2 + 1 || $k1 === null) {
			$l2 = $k2;
			$diff[] = current($old2);
			$k2 = next($old2) ? key($old2) : null;
		} else if ($k1 < $k2) {
			$l1 = $k1;
			$diff[] = current($old1);
			$k1 = next($old1) ? key($old1) : null;
		} else {
			$l2 = $k2;
			$diff[] = current($old2);
			$k2 = next($old2) ? key($old2) : null;
		}
	}

	while ($idx1 < $cnt1) {
		$diff[] = sprintf("%03d- ", $idx1 + 1) . $w[$idx1++];
	}

	while ($idx2 < $cnt2) {
		$diff[] = sprintf("%03d+ ", $idx2 + 1) . $ar2[$idx2++];
	}

	return $diff;
}

function generate_diff($wanted, $wanted_re, $output)
{
	$w = explode("\n", $wanted);
	$o = explode("\n", $output);
	$r = is_null($wanted_re) ? $w : explode("\n", $wanted_re);
	$diff = generate_array_diff($r, $o, !is_null($wanted_re), $w);

	return implode("\r\n", $diff);
}

function error($message)
{
	echo "ERROR: {$message}\n";
	exit(1);
}

function settings2array($settings, &$ini_settings)
{
	foreach($settings as $setting) {

		if (strpos($setting, '=') !== false) {
			$setting = explode("=", $setting, 2);
			$name = trim($setting[0]);
			$value = trim($setting[1]);

			if ($name == 'extension' || $name == 'zend_extension') {

				if (!isset($ini_settings[$name])) {
					$ini_settings[$name] = array();
				}

				$ini_settings[$name][] = $value;

			} else {
				$ini_settings[$name] = $value;
			}
		}
	}
}

function settings2params(&$ini_settings)
{
	$settings = '';

	foreach($ini_settings as $name => $value) {

		if (is_array($value)) {
			foreach($value as $val) {
				$val = addslashes($val);
				$settings .= " -d \"$name=$val\"";
			}
		} else {
			if (substr(PHP_OS, 0, 3) == "WIN" && !empty($value) && $value{0} == '"') {
				$len = strlen($value);

				if ($value{$len - 1} == '"') {
					$value{0} = "'";
					$value{$len - 1} = "'";
				}
			} else {
				$value = addslashes($value);
			}

			$settings .= " -d \"$name=$value\"";
		}
	}

	$ini_settings = $settings;
}

function compute_summary()
{
	global $n_total, $test_results, $ignored_by_ext, $sum_results, $percent_results;

	$n_total = count($test_results);
	$n_total += $ignored_by_ext;
	$sum_results = array(
		'PASSED'  => 0,
		'WARNED'  => 0,
		'SKIPPED' => 0,
		'FAILED'  => 0,
		'BORKED'  => 0,
		'LEAKED'  => 0,
		'XFAILED' => 0
	);

	foreach ($test_results as $v) {
		$sum_results[$v]++;
	}

	$sum_results['SKIPPED'] += $ignored_by_ext;
	$percent_results = array();

	foreach ($sum_results as $v => $n) {
		$percent_results[$v] = (100.0 * $n) / $n_total;
	}
}

function get_summary($show_ext_summary, $show_html)
{
	global $exts_skipped, $exts_tested, $n_total, $sum_results, $percent_results, $end_time, $start_time, $failed_test_summary, $PHP_FAILED_TESTS, $leak_check;

	$x_total = $n_total - $sum_results['SKIPPED'] - $sum_results['BORKED'];

	if ($x_total) {
		$x_warned = (100.0 * $sum_results['WARNED']) / $x_total;
		$x_failed = (100.0 * $sum_results['FAILED']) / $x_total;
		$x_xfailed = (100.0 * $sum_results['XFAILED']) / $x_total;
		$x_leaked = (100.0 * $sum_results['LEAKED']) / $x_total;
		$x_passed = (100.0 * $sum_results['PASSED']) / $x_total;
	} else {
		$x_warned = $x_failed = $x_passed = $x_leaked = $x_xfailed = 0;
	}

	$summary = '';

	if ($show_html) {
		$summary .= "<pre>\n";
	}

	if ($show_ext_summary) {
		$summary .= '
=====================================================================
TEST RESULT SUMMARY
---------------------------------------------------------------------
Exts skipped    : ' . sprintf('%4d', $exts_skipped) . '
Exts tested     : ' . sprintf('%4d', $exts_tested) . '
---------------------------------------------------------------------
';
	}

	$summary .= '
Number of tests : ' . sprintf('%4d', $n_total) . '          ' . sprintf('%8d', $x_total);

	if ($sum_results['BORKED']) {
		$summary .= '
Tests borked    : ' . sprintf('%4d (%5.1f%%)', $sum_results['BORKED'], $percent_results['BORKED']) . ' --------';
	}

	$summary .= '
Tests skipped   : ' . sprintf('%4d (%5.1f%%)', $sum_results['SKIPPED'], $percent_results['SKIPPED']) . ' --------
Tests warned    : ' . sprintf('%4d (%5.1f%%)', $sum_results['WARNED'], $percent_results['WARNED']) . ' ' . sprintf('(%5.1f%%)', $x_warned) . '
Tests failed    : ' . sprintf('%4d (%5.1f%%)', $sum_results['FAILED'], $percent_results['FAILED']) . ' ' . sprintf('(%5.1f%%)', $x_failed) . '
Expected fail   : ' . sprintf('%4d (%5.1f%%)', $sum_results['XFAILED'], $percent_results['XFAILED']) . ' ' . sprintf('(%5.1f%%)', $x_xfailed);

	if ($leak_check) {
		$summary .= '
Tests leaked    : ' . sprintf('%4d (%5.1f%%)', $sum_results['LEAKED'], $percent_results['LEAKED']) . ' ' . sprintf('(%5.1f%%)', $x_leaked);
	}

	$summary .= '
Tests passed    : ' . sprintf('%4d (%5.1f%%)', $sum_results['PASSED'], $percent_results['PASSED']) . ' ' . sprintf('(%5.1f%%)', $x_passed) . '
---------------------------------------------------------------------
Time taken      : ' . sprintf('%4d seconds', $end_time - $start_time) . '
=====================================================================
';
	$failed_test_summary = '';

	if (count($PHP_FAILED_TESTS['SLOW'])) {
		usort($PHP_FAILED_TESTS['SLOW'], function($a, $b) {
			return $a['info'] < $b['info'] ? 1 : -1;
		});

		$failed_test_summary .= '
=====================================================================
SLOW TEST SUMMARY
---------------------------------------------------------------------
';
		foreach ($PHP_FAILED_TESTS['SLOW'] as $failed_test_data) {
			$failed_test_summary .= sprintf('(%.3f s) ', $failed_test_data['info']) . $failed_test_data['test_name'] . "\n";
		}
		$failed_test_summary .=  "=====================================================================\n";
	}

	if (count($PHP_FAILED_TESTS['XFAILED'])) {
		$failed_test_summary .= '
=====================================================================
EXPECTED FAILED TEST SUMMARY
---------------------------------------------------------------------
';
		foreach ($PHP_FAILED_TESTS['XFAILED'] as $failed_test_data) {
			$failed_test_summary .= $failed_test_data['test_name'] . $failed_test_data['info'] . "\n";
		}
		$failed_test_summary .=  "=====================================================================\n";
	}

	if (count($PHP_FAILED_TESTS['BORKED'])) {
		$failed_test_summary .= '
=====================================================================
BORKED TEST SUMMARY
---------------------------------------------------------------------
';
		foreach ($PHP_FAILED_TESTS['BORKED'] as $failed_test_data) {
			$failed_test_summary .= $failed_test_data['info'] . "\n";
		}

		$failed_test_summary .=  "=====================================================================\n";
	}

	if (count($PHP_FAILED_TESTS['FAILED'])) {
		$failed_test_summary .= '
=====================================================================
FAILED TEST SUMMARY
---------------------------------------------------------------------
';
		foreach ($PHP_FAILED_TESTS['FAILED'] as $failed_test_data) {
			$failed_test_summary .= $failed_test_data['test_name'] . $failed_test_data['info'] . "\n";
		}
		$failed_test_summary .=  "=====================================================================\n";
	}
	if (count($PHP_FAILED_TESTS['WARNED'])) {
		$failed_test_summary .= '
=====================================================================
WARNED TEST SUMMARY
---------------------------------------------------------------------
';
		foreach ($PHP_FAILED_TESTS['WARNED'] as $failed_test_data) {
			$failed_test_summary .= $failed_test_data['test_name'] . $failed_test_data['info'] . "\n";
		}

		$failed_test_summary .=  "=====================================================================\n";
	}

	if (count($PHP_FAILED_TESTS['LEAKED'])) {
		$failed_test_summary .= '
=====================================================================
LEAKED TEST SUMMARY
---------------------------------------------------------------------
';
		foreach ($PHP_FAILED_TESTS['LEAKED'] as $failed_test_data) {
			$failed_test_summary .= $failed_test_data['test_name'] . $failed_test_data['info'] . "\n";
		}

		$failed_test_summary .=  "=====================================================================\n";
	}

	if ($failed_test_summary && !getenv('NO_PHPTEST_SUMMARY')) {
		$summary .= $failed_test_summary;
	}

	if ($show_html) {
		$summary .= "</pre>";
	}

	return $summary;
}

function show_start($start_time)
{
	global $html_output, $html_file;

	if ($html_output) {
		fwrite($html_file, "<h2>Time Start: " . date('Y-m-d H:i:s', $start_time) . "</h2>\n");
		fwrite($html_file, "<table>\n");
	}

	echo "TIME START " . date('Y-m-d H:i:s', $start_time) . "\n=====================================================================\n";
}

function show_end($end_time)
{
	global $html_output, $html_file;

	if ($html_output) {
		fwrite($html_file, "</table>\n");
		fwrite($html_file, "<h2>Time End: " . date('Y-m-d H:i:s', $end_time) . "</h2>\n");
	}

	echo "=====================================================================\nTIME END " . date('Y-m-d H:i:s', $end_time) . "\n";
}

function show_summary()
{
	global $html_output, $html_file;

	if ($html_output) {
		fwrite($html_file, "<hr/>\n" . get_summary(true, true));
	}

	echo get_summary(true, false);
}

function show_redirect_start($tests, $tested, $tested_file)
{
	global $html_output, $html_file, $line_length, $SHOW_ONLY_GROUPS;

	if ($html_output) {
		fwrite($html_file, "<tr><td colspan='3'>---&gt; $tests ($tested [$tested_file]) begin</td></tr>\n");
	}

	if (!$SHOW_ONLY_GROUPS || in_array('REDIRECT', $SHOW_ONLY_GROUPS)) {
		   echo "REDIRECT $tests ($tested [$tested_file]) begin\n";
	} else {
		   // Write over the last line to avoid random trailing chars on next echo
		   echo str_repeat(" ", $line_length), "\r";
	}
}

function show_redirect_ends($tests, $tested, $tested_file)
{
	global $html_output, $html_file, $line_length, $SHOW_ONLY_GROUPS;

	if ($html_output) {
		fwrite($html_file, "<tr><td colspan='3'>---&gt; $tests ($tested [$tested_file]) done</td></tr>\n");
	}

	if (!$SHOW_ONLY_GROUPS || in_array('REDIRECT', $SHOW_ONLY_GROUPS)) {
		   echo "REDIRECT $tests ($tested [$tested_file]) done\n";
	} else {
		   // Write over the last line to avoid random trailing chars on next echo
		   echo str_repeat(" ", $line_length), "\r";
	}
}

function show_test($test_idx, $shortname)
{
	global $test_cnt;
	global $line_length;

	$str = "TEST $test_idx/$test_cnt [$shortname]\r";
	$line_length = strlen($str);
	echo $str;
	flush();
}

function show_result($result, $tested, $tested_file, $extra = '', $temp_filenames = null)
{
	global $html_output, $html_file, $temp_target, $temp_urlbase, $line_length, $SHOW_ONLY_GROUPS;

	if (!$SHOW_ONLY_GROUPS || in_array($result, $SHOW_ONLY_GROUPS)) {
		echo "$result $tested [$tested_file] $extra\n";
	} else if (!$SHOW_ONLY_GROUPS) {
		// Write over the last line to avoid random trailing chars on next echo
		echo str_repeat(" ", $line_length), "\r";
	}

	if ($html_output) {

		if (isset($temp_filenames['file']) && file_exists($temp_filenames['file'])) {
			$url = str_replace($temp_target, $temp_urlbase, $temp_filenames['file']);
			$tested = "<a href='$url'>$tested</a>";
		}

		if (isset($temp_filenames['skip']) && file_exists($temp_filenames['skip'])) {

			if (empty($extra)) {
				$extra = "skipif";
			}

			$url = str_replace($temp_target, $temp_urlbase, $temp_filenames['skip']);
			$extra = "<a href='$url'>$extra</a>";

		} else if (empty($extra)) {
			$extra = "&nbsp;";
		}

		if (isset($temp_filenames['diff']) && file_exists($temp_filenames['diff'])) {
			$url = str_replace($temp_target, $temp_urlbase, $temp_filenames['diff']);
			$diff = "<a href='$url'>diff</a>";
		} else {
			$diff = "&nbsp;";
		}

		if (isset($temp_filenames['mem']) && file_exists($temp_filenames['mem'])) {
			$url = str_replace($temp_target, $temp_urlbase, $temp_filenames['mem']);
			$mem = "<a href='$url'>leaks</a>";
		} else {
			$mem = "&nbsp;";
		}

		fwrite($html_file,
			"<tr>" .
			"<td>$result</td>" .
			"<td>$tested</td>" .
			"<td>$extra</td>" .
			"<td>$diff</td>" .
			"<td>$mem</td>" .
			"</tr>\n");
	}
}

function junit_init() {
	// Check whether a junit log is wanted.
	$JUNIT = getenv('TEST_PHP_JUNIT');
	if (empty($JUNIT)) {
		$JUNIT = FALSE;
	} elseif (!$fp = fopen($JUNIT, 'w')) {
		error("Failed to open $JUNIT for writing.");
	} else {
		$JUNIT = array(
			'fp'            => $fp,
			'name'          => 'php-src',
			'test_total'    => 0,
			'test_pass'     => 0,
			'test_fail'     => 0,
			'test_error'    => 0,
			'test_skip'     => 0,
			'test_warn'     => 0,
			'execution_time'=> 0,
			'suites'        => array(),
			'files'         => array()
		);
	}

	$GLOBALS['JUNIT'] = $JUNIT;
}

function junit_save_xml() {
	global $JUNIT;
	if (!junit_enabled()) return;

	$xml = '<?xml version="1.0" encoding="UTF-8"?>'. PHP_EOL .
		   '<testsuites>' . PHP_EOL;
	$xml .= junit_get_suite_xml();
	$xml .= '</testsuites>';
	fwrite($JUNIT['fp'], $xml);
}

function junit_get_suite_xml($suite_name = '') {
	global $JUNIT;

	$suite = $suite_name ? $JUNIT['suites'][$suite_name] : $JUNIT;

    $result = sprintf(
		'<testsuite name="%s" tests="%s" failures="%d" errors="%d" skip="%d" time="%s">' . PHP_EOL,
        $suite['name'], $suite['test_total'], $suite['test_fail'], $suite['test_error'], $suite['test_skip'],
		$suite['execution_time']
	);

	foreach($suite['suites'] as $sub_suite) {
		$result .= junit_get_suite_xml($sub_suite['name']);
	}

	// Output files only in subsuites
	if (!empty($suite_name)) {
		foreach($suite['files'] as $file) {
			$result .= $JUNIT['files'][$file]['xml'];
		}
	}

	$result .= '</testsuite>' . PHP_EOL;

	return $result;
}

function junit_enabled() {
	global $JUNIT;
	return !empty($JUNIT);
}

/**
 * @param array|string $type
 * @param string $file_name
 * @param string $test_name
 * @param int|string $time
 * @param string $message
 * @param string $details
 * @return void
 */
function junit_mark_test_as($type, $file_name, $test_name, $time = null, $message = '', $details = '') {
	global $JUNIT;
	if (!junit_enabled()) return;

	$suite = junit_get_suitename_for($file_name);

	junit_suite_record($suite, 'test_total');

	$time = null !== $time ? $time : junit_get_timer($file_name);
	junit_suite_record($suite, 'execution_time', $time);

	$escaped_details = htmlspecialchars($details, ENT_QUOTES, 'UTF-8');
	$escaped_details = preg_replace_callback('/[\0-\x08\x0B\x0C\x0E-\x1F]/', function ($c) {
		return sprintf('[[0x%02x]]', ord($c[0]));
	}, $escaped_details);
	$escaped_message = htmlspecialchars($message, ENT_QUOTES, 'UTF-8');

    $escaped_test_name = basename($file_name) . ' - ' . htmlspecialchars($test_name, ENT_QUOTES);
    $JUNIT['files'][$file_name]['xml'] = "<testcase classname='$suite' name='$escaped_test_name' time='$time'>\n";

	if (is_array($type)) {
		$output_type = $type[0] . 'ED';
		$temp = array_intersect(array('XFAIL', 'FAIL', 'WARN'), $type);
		$type = reset($temp);
	} else {
		$output_type = $type . 'ED';
	}

	if ('PASS' == $type || 'XFAIL' == $type) {
		junit_suite_record($suite, 'test_pass');
	} elseif ('BORK' == $type) {
		junit_suite_record($suite, 'test_error');
		$JUNIT['files'][$file_name]['xml'] .= "<error type='$output_type' message='$escaped_message'/>\n";
	} elseif ('SKIP' == $type) {
		junit_suite_record($suite, 'test_skip');
		$JUNIT['files'][$file_name]['xml'] .= "<skipped>$escaped_message</skipped>\n";
	} elseif ('WARN' == $type) {
		junit_suite_record($suite, 'test_warn');
		$JUNIT['files'][$file_name]['xml'] .= "<warning>$escaped_message</warning>\n";
	} elseif ('FAIL' == $type) {
		junit_suite_record($suite, 'test_fail');
		$JUNIT['files'][$file_name]['xml'] .= "<failure type='$output_type' message='$escaped_message'>$escaped_details</failure>\n";
	} else {
		junit_suite_record($suite, 'test_error');
		$JUNIT['files'][$file_name]['xml'] .= "<error type='$output_type' message='$escaped_message'>$escaped_details</error>\n";
	}

	$JUNIT['files'][$file_name]['xml'] .= "</testcase>\n";

}

function junit_suite_record($suite, $param, $value = 1) {
	global $JUNIT;

	$JUNIT[$param] += $value;
	$JUNIT['suites'][$suite][$param] += $value;
}

function junit_get_timer($file_name) {
	global $JUNIT;
	if (!junit_enabled()) return 0;

	if (isset($JUNIT['files'][$file_name]['total'])) {
		return number_format($JUNIT['files'][$file_name]['total'], 4);
	}

	return 0;
}

function junit_start_timer($file_name) {
	global $JUNIT;
	if (!junit_enabled()) return;

	if (!isset($JUNIT['files'][$file_name]['start'])) {
		$JUNIT['files'][$file_name]['start'] = microtime(true);

		$suite = junit_get_suitename_for($file_name);
		junit_init_suite($suite);
		$JUNIT['suites'][$suite]['files'][$file_name] = $file_name;
	}
}

function junit_get_suitename_for($file_name) {
	return junit_path_to_classname(dirname($file_name));
}

function junit_path_to_classname($file_name) {
    global $JUNIT;
    return $JUNIT['name'] . '.' . str_replace(DIRECTORY_SEPARATOR, '.', $file_name);
}

function junit_init_suite($suite_name) {
	global $JUNIT;
	if (!junit_enabled()) return;

	if (!empty($JUNIT['suites'][$suite_name])) {
		return;
	}

	$JUNIT['suites'][$suite_name] = array(
		'name'          => $suite_name,
		'test_total'    => 0,
		'test_pass'     => 0,
		'test_fail'     => 0,
		'test_error'    => 0,
		'test_skip'     => 0,
		'suites'        => array(),
		'files'         => array(),
		'execution_time'=> 0,
	);
}

function junit_finish_timer($file_name) {
	global $JUNIT;
	if (!junit_enabled()) return;

	if (!isset($JUNIT['files'][$file_name]['start'])) {
		error("Timer for $file_name was not started!");
	}

	if (!isset($JUNIT['files'][$file_name]['total'])) {
        $JUNIT['files'][$file_name]['total'] = 0;
    }

	$start = $JUNIT['files'][$file_name]['start'];
	$JUNIT['files'][$file_name]['total'] += microtime(true) - $start;
	unset($JUNIT['files'][$file_name]['start']);
}

/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim: noet sw=4 ts=4
 */
?>
