import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AtualizaBancoDeDados {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/seu_banco_de_dados";
    private static final String USUARIO = "seu_usuario";
    private static final String SENHA = "sua_senha";
    private static final String STRING_BUSCA = "https://player.vimeo.com/video/";
    private static final String TEMPLATE_SUBSTITUICAO = "https://beyond.spalla.io/player/?video=3333149b-3333-4bb6-aaaa-000%s";

    public static void main(String[] args) {
        Connection conn = null;
        PreparedStatement pstmtSelect = null;
        PreparedStatement pstmtUpdate = null;
        ResultSet rs = null;

        try {
            // Conecta ao banco de dados
            conn = DriverManager.getConnection(DB_URL, USUARIO, SENHA);

            // Seleciona os registros que precisam ser atualizados
            String sqlSelect = "SELECT id, sua_coluna FROM sua_tabela WHERE sua_coluna LIKE ?";
            pstmtSelect = conn.prepareStatement(sqlSelect);
            pstmtSelect.setString(1, "%" + STRING_BUSCA + "%");
            rs = pstmtSelect.executeQuery();

            // Prepara a instrução de atualização
            String sqlUpdate = "UPDATE sua_tabela SET sua_coluna = ? WHERE id = ?";
            pstmtUpdate = conn.prepareStatement(sqlUpdate);

            while (rs.next()) {
                int id = rs.getInt("id");
                String dadosColuna = rs.getString("sua_coluna");

                // Extrai o ID do Vimeo
                int inicioIndice = dadosColuna.indexOf(STRING_BUSCA) + STRING_BUSCA.length();
                int fimIndice = dadosColuna.indexOf("?", inicioIndice);
                String idVimeo = dadosColuna.substring(inicioIndice, fimIndice);

                // Constrói a nova URL de substituição
                String novaURL = String.format(TEMPLATE_SUBSTITUICAO, idVimeo);

                // Substitui o conteúdo dentro das aspas do src
                String novosDadosColuna = dadosColuna.replaceAll(
                        "https://player.vimeo.com/video/" + idVimeo + "\\?badge=0&amp;autopause=0&amp;player_id=0&amp;app_id=58479",
                        novaURL);

                // Executa a atualização
                pstmtUpdate.setString(1, novosDadosColuna);
                pstmtUpdate.setInt(2, id);
                pstmtUpdate.executeUpdate();
            }

            System.out.println("Banco de dados atualizado com sucesso.");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) rs.close();
                if (pstmtSelect != null) pstmtSelect.close();
                if (pstmtUpdate != null) pstmtUpdate.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
