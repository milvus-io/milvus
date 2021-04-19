timeout(time: 10, unit: 'MINUTES') {
    dir ("scripts") {
        sh '. ./before-install.sh && unset http_proxy && unset https_proxy && ./check_cache.sh -l $CCACHE_ARTFACTORY_URL --cache_dir=\$CCACHE_DIR -f ccache-\$OS_NAME-\$BUILD_ENV_IMAGE_ID.tar.gz || echo \"ccache artfactory files not found!\"'
        sh '. ./before-install.sh && unset http_proxy && unset https_proxy && ./check_cache.sh -l $THIRDPARTY_ARTFACTORY_URL --cache_dir=$CUSTOM_THIRDPARTY_PATH -f thirdparty-download.tar.gz || echo \"thirdparty artfactory files not found!\"'
    }

    sh '. ./scripts/before-install.sh && make install'

    dir ("scripts") {
        withCredentials([usernamePassword(credentialsId: "${env.JFROG_CREDENTIALS_ID}", usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
            sh '. ./before-install.sh && unset http_proxy && unset https_proxy && ./update_cache.sh -l $CCACHE_ARTFACTORY_URL --cache_dir=\$CCACHE_DIR -f ccache-\$OS_NAME-\$BUILD_ENV_IMAGE_ID.tar.gz -u ${USERNAME} -p ${PASSWORD}'
            sh '. ./before-install.sh && unset http_proxy && unset https_proxy && ./update_cache.sh -l $THIRDPARTY_ARTFACTORY_URL --cache_dir=$CUSTOM_THIRDPARTY_PATH -f thirdparty-download.tar.gz -u ${USERNAME} -p ${PASSWORD}'
        }
    }
}
