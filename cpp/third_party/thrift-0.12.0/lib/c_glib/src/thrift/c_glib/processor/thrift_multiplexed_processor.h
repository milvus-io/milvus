/*
 * thrift_multiplexed_processor.h
 *
 *  Created on: 14 sept. 2017
 *      Author: gaguilar
 */

#ifndef _THRIFT_MULTIPLEXED_MULTIPLEXED_PROCESSOR_H_
#define _THRIFT_MULTIPLEXED_MULTIPLEXED_PROCESSOR_H_


#include <glib-object.h>

#include <thrift/c_glib/processor/thrift_processor.h>


G_BEGIN_DECLS

/*! \file thrift_multiplexed_processor.h
 *  \brief The multiplexed processor for c_glib.
 */

/* type macros */
#define THRIFT_TYPE_MULTIPLEXED_PROCESSOR (thrift_multiplexed_processor_get_type ())
#define THRIFT_MULTIPLEXED_PROCESSOR(obj) (G_TYPE_CHECK_INSTANCE_CAST ((obj), THRIFT_TYPE_MULTIPLEXED_PROCESSOR, ThriftMultiplexedProcessor))
#define THRIFT_IS_MULTIPLEXED_PROCESSOR(obj) (G_TYPE_CHECK_INSTANCE_TYPE ((obj), THRIFT_TYPE_MULTIPLEXED_PROCESSOR))
#define THRIFT_MULTIPLEXED_PROCESSOR_CLASS(c) (G_TYPE_CHECK_CLASS_CAST ((c), THRIFT_TYPE_MULTIPLEXED_PROCESSOR, ThriftMultiplexedProcessorClass))
#define THRIFT_IS_MULTIPLEXED_PROCESSOR_CLASS(c) (G_TYPE_CHECK_CLASS_TYPE ((c), THRIFT_TYPE_MULTIPLEXED_PROCESSOR))
#define THRIFT_MULTIPLEXED_PROCESSOR_GET_CLASS(obj) (G_TYPE_INSTANCE_GET_CLASS ((obj), THRIFT_TYPE_MULTIPLEXED_PROCESSOR, ThriftMultiplexedProcessorClass))

/* define the GError domain string */
#define THRIFT_MULTIPLEXED_PROCESSOR_ERROR_DOMAIN "thrift-multiplexed-processor-error-quark"


/*!
 * Thrift MultiplexedProcessor object
 */
struct _ThriftMultiplexedProcessor
{
  ThriftProcessor parent;

  /* private */
  gchar * default_processor_name;
  GHashTable *multiplexed_services;
};
typedef struct _ThriftMultiplexedProcessor ThriftMultiplexedProcessor;

/*!
 * Thrift MultiplexedProcessor class
 */
struct _ThriftMultiplexedProcessorClass
{
  ThriftProcessorClass parent;

  gboolean (* register_processor) (ThriftProcessor *self, const gchar * multiplexed_processor_name, ThriftProcessor * multiplexed_processor , GError **error);

};
typedef struct _ThriftMultiplexedProcessorClass ThriftMultiplexedProcessorClass;

/* used by THRIFT_TYPE_MULTIPLEXED_PROCESSOR */
GType thrift_multiplexed_processor_get_type (void);

/*!
 * Processes the request.
 * \public \memberof ThriftMultiplexedProcessorClass
 */
gboolean thrift_multiplexed_processor_process (ThriftMultiplexedProcessor *processor,
                                   ThriftProtocol *in, ThriftProtocol *out,
                                   GError **error);


/* Public API */

/**
 * @brief Registers a processor in the multiplexed processor under its name. It
 * will take a reference to the processor so refcount will be incremented.
 * It will also be decremented on object destruction.
 *
 * The first registered processor becomes default. But you can override it with
 * "default" property.
 *
 * It returns a compliant error if it cannot be registered.
 *
 * @param processor Pointer to the multiplexed processor.
 * @param multiplexed_processor_name Name of the processor you want to register
 * @param multiplexed_processor Pointer to implemented processor you want multiplex.
 * @param error Error object where we should store errors.
 *
 * @see https://developer.gnome.org/glib/stable/glib-Error-Reporting.html#g-set-error
 */
gboolean thrift_multiplexed_processor_register_processor(ThriftProcessor *processor, const gchar * multiplexed_processor_name, ThriftProcessor * multiplexed_processor , GError **error);


/* define error/exception types */
typedef enum
{
  THRIFT_MULTIPLEXED_PROCESSOR_ERROR_UNKNOWN,
  THRIFT_MULTIPLEXED_PROCESSOR_ERROR_SERVICE_UNAVAILABLE,
  THRIFT_MULTIPLEXED_PROCESSOR_ERROR_MESSAGE_TYPE,
  THRIFT_MULTIPLEXED_PROCESSOR_ERROR_MESSAGE_WRONGLY_MULTIPLEXED,
  THRIFT_MULTIPLEXED_PROCESSOR_ERROR_SEND,
  THRIFT_MULTIPLEXED_PROCESSOR_ERROR_RECEIVE,
  THRIFT_MULTIPLEXED_PROCESSOR_ERROR_CLOSE
} ThriftMultiplexedProcessorError;


GQuark thrift_multiplexed_processor_error_quark (void);
#define THRIFT_MULTIPLEXED_PROCESSOR_ERROR (thrift_multiplexed_processor_error_quark ())


G_END_DECLS


#endif /* _THRIFT_MULTIPLEXED_MULTIPLEXED_PROCESSOR_H_ */
