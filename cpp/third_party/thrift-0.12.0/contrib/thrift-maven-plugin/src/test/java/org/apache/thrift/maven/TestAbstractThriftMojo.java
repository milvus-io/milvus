/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.thrift.maven;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;


public class TestAbstractThriftMojo {

    private ThriftTestCompileMojo mojo;
    private File testRootDir;
    private ArtifactRepository mavenRepository;


    @Before
    public void setUp() throws Exception {
        final File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        testRootDir = new File(tmpDir, "thrift-test");

        // the truncatePath method assumes a maven repository, but it only cares about the base dir
        mavenRepository = Mockito.mock(ArtifactRepository.class);
        Mockito.when(mavenRepository.getBasedir()).thenReturn("/test/maven/repo/basedir");

        mojo = new ThriftTestCompileMojo();
        mojo.setLocalMavenRepository(mavenRepository);
    }

    @Test
    public void testMakeThriftPathFromJars() throws Throwable {
        final File temporaryThriftFileDirectory = testRootDir;

        // The SharedIdl.jar file contains the same idl/shared.thrift and idl/tutorial.thrift hierarchy
        // used by other tests. It's used here to represent a dependency of the project maven is building,
        // one that is contributing .thrift IDL files as well as any other artifacts.
        final Iterable<File> classpathElementFiles = Lists.newArrayList(
                new File("src/test/resources/dependency-jar-test/SharedIdl.jar")
        );

        final Set<File> thriftDirectories = mojo.makeThriftPathFromJars(temporaryThriftFileDirectory, classpathElementFiles);

        // The results should be a path to a directory named after the JAR itself (assuming no path hashing,
        // but see below for a separate test of that) representing the root of a hierarchy containing thrift
        // files, suitable for providing to the thrift compiler as an include directory. In this case, that
        // means it points to the directory containing the "idl" hierarchy rather than to the idl directory
        // itself.
        final Set<File> expected = Sets.newHashSet(
                new File(testRootDir, "src/test/resources/dependency-jar-test/SharedIdl.jar")
        );

        assertEquals("makeThriftPathFromJars should return thrift IDL base path from within JAR", expected, thriftDirectories);
    }

    @Test
    public void testTruncatePath() throws Throwable {
        // JAR path is unrelated to maven repo, and should be unchanged
        assertEquals("/path/to/somejar.jar", mojo.truncatePath("/path/to/somejar.jar"));

        // JAR path is within maven repo, and should be made relative to the repo
        assertEquals("path/to/somejar.jar", mojo.truncatePath("/test/maven/repo/basedir/path/to/somejar.jar"));

        // JAR path contains forward slashes that should be normalized
        assertEquals("/path/to/somejar.jar", mojo.truncatePath("\\path\\to\\somejar.jar"));
    }

    @Test
    public void testTruncatePathWithDependentPathHashing() throws Throwable {
        mojo.setHashDependentPaths(true);

        // hashDependentPaths set to true, the JAR path is immediately hashed (MD5) and converted to a hex string

        assertEquals("1c85950987b23493462cf3c261d9510a", mojo.truncatePath("/path/to/somejar.jar"));
        assertEquals("39fc2b4c34cb6cb0da38bed5d8b5fc67", mojo.truncatePath("/test/maven/repo/basedir/path/to/somejar.jar"));
        assertEquals("25b6924f5b0e19486d0ff88448e999d5", mojo.truncatePath("\\path\\to\\somejar.jar"));
    }

}
