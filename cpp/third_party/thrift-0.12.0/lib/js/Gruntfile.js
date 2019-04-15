//To build dist/thrift.js, dist/thrift.min.js and doc/*
//run grunt at the command line in this directory.
//Prerequisites:
// Node Setup -   nodejs.org
// Grunt Setup -  npm install  //reads the ./package.json and installs project dependencies
// Run grunt -    npx grunt  // uses project-local installed version of grunt (from package.json)

module.exports = function(grunt) {
  'use strict';

  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),
    concat: {
      options: {
        separator: ';'
      },
      dist: {
        src: ['src/**/*.js'],
        dest: 'dist/<%= pkg.name %>.js'
      }
    },
    jsdoc : {
        dist : {
            src: ['src/*.js', './README.md'],
            options: {
              destination: 'doc'
            }
        }
    },
    uglify: {
      options: {
        banner: '/*! <%= pkg.name %> <%= grunt.template.today("dd-mm-yyyy") %> */\n'
      },
      dist: {
        files: {
          'dist/<%= pkg.name %>.min.js': ['<%= concat.dist.dest %>']
        }
      }
    },
    shell: {
      InstallThriftJS: {
        command: 'mkdir -p test/build/js/lib; cp src/thrift.js test/build/js/thrift.js'
      },
      InstallThriftNodeJSDep: {
        command: 'cd ../..; npm install'
      },
      InstallTestLibs: {
        command: 'cd test; ant download_jslibs'
      },
      ThriftGen: {
        command: [
          'mkdir -p test/gen-js',
          '../../compiler/cpp/thrift -gen js --out test/gen-js ../../test/ThriftTest.thrift',
          '../../compiler/cpp/thrift -gen js --out test/gen-js ../../test/JsDeepConstructorTest.thrift',
          'mkdir -p test/gen-js-jquery',
          '../../compiler/cpp/thrift -gen js:jquery --out test/gen-js-jquery ../../test/ThriftTest.thrift',
          'mkdir -p test/gen-nodejs',
          '../../compiler/cpp/thrift -gen js:node --out test/gen-nodejs ../../test/ThriftTest.thrift',
          'mkdir -p test/gen-js-es6',
          '../../compiler/cpp/thrift -gen js:es6 --out test/gen-js-es6 ../../test/ThriftTest.thrift',
          'mkdir -p test/gen-nodejs-es6',
          '../../compiler/cpp/thrift -gen js:node,es6 --out ./test/gen-nodejs-es6 ../../test/ThriftTest.thrift',
        ].join(' && ')
      },
      ThriftGenJQ: {
        command: '../../compiler/cpp/thrift -gen js:jquery -gen js:node -o test ../../test/ThriftTest.thrift'
      },
      ThriftGenDeepConstructor: {
        command: '../../compiler/cpp/thrift -gen js -o test ../../test/JsDeepConstructorTest.thrift'
      },
      ThriftGenDoubleConstants: {
        command: '../../compiler/cpp/thrift -gen js -o test ../../test/DoubleConstantsTest.thrift'
      },
      ThriftTestServer: {
        options: {
          async: true,
          execOptions: {
            cwd: "./test",
            env: {NODE_PATH: "../../nodejs/lib:../../../node_modules"}
          }
        },
        command: "node server_http.js",
      },
      ThriftTestServerES6: {
        options: {
          async: true,
          execOptions: {
            cwd: "./test",
            env: {NODE_PATH: "../../nodejs/lib:../../../node_modules"}
          }
        },
        command: "node server_http.js --es6",
      },
      ThriftTestServer_TLS: {
        options: {
          async: true,
          execOptions: {
            cwd: "./test",
            env: {NODE_PATH: "../../nodejs/lib:../../../node_modules"}
          }
        },
        command: "node server_https.js",
      },
      ThriftTestServerES6_TLS: {
        options: {
          async: true,
          execOptions: {
            cwd: "./test",
            env: {NODE_PATH: "../../nodejs/lib:../../../node_modules"}
          }
        },
        command: "node server_https.js --es6",
      },
    },
    qunit: {
      ThriftJS: {
        options: {
          urls: [
            'http://localhost:8089/test-nojq.html'
          ],
          puppeteer: {
            headless: true,
            args: ['--no-sandbox'],
          },
        }
      },
      ThriftJSJQ: {
        options: {
          urls: [
            'http://localhost:8089/test.html'
          ],
          puppeteer: {
            headless: true,
            args: ['--no-sandbox'],
          },
        }
      },
      ThriftJS_DoubleRendering: {
        options: {
          urls: [
            'http://localhost:8089/test-double-rendering.html'
          ],
          puppeteer: {
            headless: true,
            args: ['--no-sandbox'],
            ignoreHTTPSErrors: true,
          },
        }
      },
      ThriftWS: {
        options: {
          urls: [
            'http://localhost:8089/testws.html'
          ],
          puppeteer: {
            headless: true,
            args: ['--no-sandbox'],
          },
        }
      },
      ThriftJS_TLS: {
        options: {
          urls: [
            'https://localhost:8091/test-nojq.html'
          ],
          puppeteer: {
            headless: true,
            args: ['--no-sandbox'],
            ignoreHTTPSErrors: true,
          },
        }
      },
      ThriftJSJQ_TLS: {
        options: {
          urls: [
            'https://localhost:8091/test.html'
          ],
          puppeteer: {
            headless: true,
            args: ['--no-sandbox'],
            ignoreHTTPSErrors: true,
          },
        }
      },
      ThriftWS_TLS: {
        options: {
          urls: [
            'https://localhost:8091/testws.html'
          ],
          puppeteer: {
            headless: true,
            args: ['--no-sandbox'],
            ignoreHTTPSErrors: true,
          },
        }
      },
      ThriftDeepConstructor: {
        options: {
          urls: [
            'http://localhost:8089/test-deep-constructor.html'
          ],
          puppeteer: {
            headless: true,
            args: ['--no-sandbox'],
          },
        }
      },
      ThriftWSES6: {
        options: {
          urls: [
            'http://localhost:8088/test-es6.html'
          ],
          puppeteer: {
            headless: true,
            args: ['--no-sandbox'],
          },
        }
      }
    },
    jshint: {
      // The main thrift library file. not es6 yet :(
      lib: {
        src: ['src/**/*.js'],
      },
      // The test files use es6
      test: {
        src: ['Gruntfile.js', 'test/*.js'],
        options: {
          esversion: 6,
        }
      },
      gen_js_code: {
        src: ['test/gen-js/*.js', 'test/gen-js-jquery/*.js'],
      },
      gen_es6_code: {
        src: ['test/gen-js-es6/*.js'],
        options: {
          esversion: 6,
        }
      },
      gen_node_code: {
        src: ['test/gen-nodejs/*.js'],
        options: {
          node: true,
        }
      },
      gen_node_es6_code: {
        src: ['test/gen-nodejs-es6/*.js'],
        options: {
          node: true,
          esversion: 6,
        }
      }
    },
  });

  grunt.loadNpmTasks('grunt-contrib-uglify');
  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-contrib-qunit');
  grunt.loadNpmTasks('grunt-contrib-concat');
  grunt.loadNpmTasks('grunt-jsdoc');
  grunt.loadNpmTasks('grunt-shell-spawn');

  grunt.registerTask('wait', 'Wait just one second for server to start', function () {
    var done = this.async();
    setTimeout(function() {
      done(true);
    }, 1000);
  });

  grunt.registerTask('installAndGenerate', [
    'shell:InstallThriftJS', 'shell:InstallThriftNodeJSDep', 'shell:ThriftGen',
    'shell:ThriftGenDeepConstructor',
    'shell:InstallTestLibs',
  ]);

  grunt.registerTask('test', [
    'installAndGenerate',
    'jshint',
    'shell:ThriftTestServer', 'shell:ThriftTestServer_TLS',
    'shell:ThriftTestServerES6', 'shell:ThriftTestServerES6_TLS',
    'wait',
    'qunit:ThriftDeepConstructor',
    'qunit:ThriftJS', 'qunit:ThriftJS_TLS',
    'qunit:ThriftWS',
    'qunit:ThriftJSJQ', 'qunit:ThriftJSJQ_TLS',
    'qunit:ThriftWSES6',
    'shell:ThriftTestServer:kill', 'shell:ThriftTestServer_TLS:kill',
    'shell:ThriftTestServerES6:kill', 'shell:ThriftTestServerES6_TLS:kill',
  ]);
  grunt.registerTask('default', ['test', 'concat', 'uglify', 'jsdoc']);
};
