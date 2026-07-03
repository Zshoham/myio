# Zephyr module glue: builds the Zephyr backend and exposes the myio
# headers to the application. Enabled with CONFIG_MYIO=y.
if(CONFIG_MYIO)
  zephyr_library_named(myio)
  zephyr_library_sources(${ZEPHYR_CURRENT_MODULE_DIR}/src/myio_zephyr.c)
  zephyr_library_sources_ifdef(CONFIG_MYIO_POOL
                               ${ZEPHYR_CURRENT_MODULE_DIR}/src/myio_zephyr_pool.c)
  zephyr_include_directories(${ZEPHYR_CURRENT_MODULE_DIR}/include)
endif()
