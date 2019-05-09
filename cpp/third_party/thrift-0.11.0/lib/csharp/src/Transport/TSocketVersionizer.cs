using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
#if NET45
using System.Threading.Tasks;
#endif

namespace Thrift.Transport
{
	/**
	* PropertyInfo for the DualMode property of the System.Net.Sockets.Socket class. Used to determine if the sockets are capable of
	* automatic IPv4 and IPv6 handling. If DualMode is present the sockets automatically handle IPv4 and IPv6 connections.
	* If the DualMode is not available the system configuration determines whether IPv4 or IPv6 is used.
	*/
	internal static class TSocketVersionizer
	{
		/*
		* Creates a TcpClient according to the capabilitites of the used framework
		*/
		internal static TcpClient CreateTcpClient()
		{
			TcpClient client = null;

#if NET45
			client = new TcpClient(AddressFamily.InterNetworkV6);
			client.Client.DualMode = true;
#else
            client = new TcpClient(AddressFamily.InterNetwork);
#endif

			return client;
		}

		/*
		* Creates a TcpListener according to the capabilitites of the used framework
		*/
		internal static TcpListener CreateTcpListener(Int32 port)
		{
			TcpListener listener = null;

#if NET45
			listener = new TcpListener(System.Net.IPAddress.IPv6Any, port);
			listener.Server.DualMode = true;
#else

			listener = new TcpListener(System.Net.IPAddress.Any, port);
#endif

            return listener;
		}
	}
}
