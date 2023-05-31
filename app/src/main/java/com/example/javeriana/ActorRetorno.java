package com.example.javeriana;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.sql.*;

public class ActorRetorno {

    private static final String DB_URL = "jdbc:mariadb://localhost:3306/biblioteca_caribe";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";

    public static void main(String[] args) {
        try (ZContext context = new ZContext()) {
            // Crea un socket de tipo REP
            ZMQ.Socket socket = context.createSocket(SocketType.REP);
            // Vincula el socket al puerto y dirección local
            socket.bind("tcp://*:5556");

            while (!Thread.currentThread().isInterrupted()) {
                // Espera una petición del gestor
                ZMsg request = ZMsg.recvMsg(socket);
                System.out.println("Mensaje recibido del gestor: " + request.getFirst().toString());

                // Procesa la petición y envía la respuesta
                String respuesta = procesarPeticion(request.getFirst().toString());
                ZMsg response = new ZMsg();
                response.add(respuesta);
                response.send(socket);
            }
        }
    }

    private static String procesarPeticion(String peticion) {
        System.out.println("Estoy solicitando la peticion....");
        String respuesta = "Peticion no reconocida";
        String comando = "Comando no reconocido";
        String tituloLibro = "TITULO NO LEIDO";
        String nombreUsuario = "USUARIO NO LEIDO";
        

        try {
            String[] datosPeticion = peticion.split(",");
            comando = datosPeticion[0];
            tituloLibro = datosPeticion[1];
            nombreUsuario = datosPeticion[2];
        } catch (ArrayIndexOutOfBoundsException e) {
            return respuesta;
        }

        switch (comando) {
            case "DEVOLUCION":
                System.out.println("Entre al case");
                respuesta = retornarLibro(tituloLibro, nombreUsuario);
                break;
            // Si hay más casos de comandos, agrégalos aquí
            default:
                return respuesta;
        }

        return respuesta;
    }

    public static String retornarLibro(String pTitulo, String pNombreUsuario) {
        
        String respuesta = "Se actualizó la base de datos. El libro " + pTitulo + " ha sido retornado por " + pNombreUsuario;
        
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            // Conectar a la base de datos
            Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            
            // Buscar libro en la base de datos
            String selectQuery = "SELECT * FROM libros WHERE titulo = ?";
            PreparedStatement selectStmt = conn.prepareStatement(selectQuery);
            selectStmt.setString(1, pTitulo);
            ResultSet result = selectStmt.executeQuery();
            
            // Lanzar una excepción si el libro no se encuentra
            if (!result.next()) {
                respuesta = "Libro no encontrado: " + pTitulo;
                return respuesta;
            }
            
            int idLibro = result.getInt("id");
            
            // Buscar usuario en la base de datos
            String selectUsuarioQuery = "SELECT * FROM usuarios WHERE nombre = ?";
            PreparedStatement selectUsuarioStmt = conn.prepareStatement(selectUsuarioQuery);
            selectUsuarioStmt.setString(1, pNombreUsuario);
            ResultSet resultUsuario = selectUsuarioStmt.executeQuery();
            
            // Lanzar una excepción si el usuario no se encuentra
            if (!resultUsuario.next()) {
                respuesta = "Usuario no encontrado: " + pNombreUsuario;
                return respuesta;
            }
            
            int idUsuario = resultUsuario.getInt("id");
            
            // Actualizar préstamo si existe y si la fecha_devolucion es NULL
            String selectPrestamoQuery = "SELECT * FROM prestamos WHERE libro_id = ? AND usuario_id = ?";
            PreparedStatement selectPrestamoStmt = conn.prepareStatement(selectPrestamoQuery);
            selectPrestamoStmt.setInt(1, idLibro);
            selectPrestamoStmt.setInt(2, idUsuario);
            ResultSet resultPrestamo = selectPrestamoStmt.executeQuery();
            
            // Lanzar una excepción si el préstamo no se encuentra
            if (!resultPrestamo.next()) {
                respuesta = "No se encontró un préstamo para el libro " + pTitulo + " y el usuario " + pNombreUsuario;
                return respuesta;
            }
            
            // Actualizar préstamo si la fecha_devolucion es NULL
            if (resultPrestamo.getString("fecha_devolucion") == null) {
                // Actualizar el estado del libro en la base de datos
                String updateQuery = "UPDATE libros SET ejemplares_disponibles = ejemplares_disponibles + 1 WHERE titulo = ?";
                PreparedStatement updateStmt = conn.prepareStatement(updateQuery);
                updateStmt.setString(1, pTitulo);
                int rowsUpdated = updateStmt.executeUpdate();
                
                String updatePrestamosQuery = "UPDATE prestamos SET fecha_devolucion = NOW() WHERE libro_id = ? AND usuario_id = ?";
                PreparedStatement updatePrestamosStmt = conn.prepareStatement(updatePrestamosQuery);
                updatePrestamosStmt.setInt(1, idLibro);
                updatePrestamosStmt.setInt(2, idUsuario);
                rowsUpdated += updatePrestamosStmt.executeUpdate();
                
                // Si ninguna fila fue actualizada, lanzar una excepción
                if (rowsUpdated == 0) {
                    respuesta = "No se logró actualizar el estado del libro: " + pTitulo;
                    return respuesta;
                }
            } else {
                respuesta = "El préstamo para el libro " + pTitulo + " y el usuario " + pNombreUsuario + " ya ha sido devuelto.";
                return respuesta;
            }
            
            // Cerrar conexión con la base de datos
            conn.close();
        
            
        } catch (SQLException e) {
            // Manejar excepciones de SQL y redireccionarlas como RemoteException para RMI
            respuesta = "No se logró retornar el libro: " + e.getMessage();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        
        return respuesta;
    }
    
}
