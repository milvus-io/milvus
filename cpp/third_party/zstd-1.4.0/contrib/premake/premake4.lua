-- Include zstd.lua in your GENie or premake4 file, which exposes a project_zstd function
dofile('zstd.lua')

solution 'example'
	configurations { 'Debug', 'Release' }
	project_zstd('../../lib/')
