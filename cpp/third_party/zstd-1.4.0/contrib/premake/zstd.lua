-- This GENie/premake file copies the behavior of the Makefile in the lib folder.
-- Basic usage: project_zstd(ZSTD_DIR)

function project_zstd(dir, compression, decompression, deprecated, dictbuilder, legacy)
	if compression == nil then compression = true end
	if decompression == nil then decompression = true end
	if deprecated == nil then deprecated = false end
	if dictbuilder == nil then dictbuilder = false end

	if legacy == nil then legacy = 0 end

	if not compression then
		dictbuilder = false
		deprecated = false
	end

	if not decompression then
		legacy = 0
		deprecated = false
	end

	project 'zstd'
		kind 'StaticLib'
		language 'C'

		files {
			dir .. 'zstd.h',
			dir .. 'common/**.c',
			dir .. 'common/**.h'
		}

		if compression then
			files {
				dir .. 'compress/**.c',
				dir .. 'compress/**.h'
			}
		end

		if decompression then
			files {
				dir .. 'decompress/**.c',
				dir .. 'decompress/**.h'
			}
		end

		if dictbuilder then
			files {
				dir .. 'dictBuilder/**.c',
				dir .. 'dictBuilder/**.h'
			}
		end

		if deprecated then
			files {
				dir .. 'deprecated/**.c',
				dir .. 'deprecated/**.h'
			}
		end

		if legacy ~= 0 then
			if legacy >= 8 then
				files {
					dir .. 'legacy/zstd_v0' .. (legacy - 7) .. '.*'
				}
			end
			includedirs {
				dir .. 'legacy'
			}
		end

		includedirs {
			dir,
			dir .. 'common'
		}

		defines {
			'XXH_NAMESPACE=ZSTD_',
			'ZSTD_LEGACY_SUPPORT=' .. legacy
		}
end
